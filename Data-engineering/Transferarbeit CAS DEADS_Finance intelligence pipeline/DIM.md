## Dimension tables

### `team.dim.umccseasoncycle` (SQL, static)

**Purpose:** maps every UMCC football season to its 3-season reporting cycle.

- Generated via a one-time `CREATE OR REFRESH TABLE ... AS` SQL statement, not a pipeline table
- Logic: cycles run in consecutive 3-season groups, anchored backward from the most recent complete cycle (`24-25, 25-26, 26-27` → `24-27`), extending back to season `90-91`
- Static reference data — not expected to change on a regular cadence; lives in the shared `team.dim` schema alongside other project dimension tables

```sql
CREATE OR REPLACE TABLE team.dim.UMCCseasoncycle AS
WITH years AS (
  SELECT explode(sequence(1990, 2026)) AS start_year
),
calc AS (
  SELECT
    start_year,
    concat(
      lpad(CAST(start_year % 100 AS STRING), 2, '0'), '-',
      lpad(CAST((start_year + 1) % 100 AS STRING), 2, '0')
    ) AS season,
    2026 - start_year AS n
  FROM years
),
grouped AS (
  SELECT
    season,
    start_year,
    floor(n / 3) AS grp
  FROM calc
)
SELECT
  season,
  concat(
    lpad(CAST((2024 - 3 * grp) % 100 AS STRING), 2, '0'), '-',
    lpad(CAST((2027 - 3 * grp) % 100 AS STRING), 2, '0')
  ) AS cycle
FROM grouped
ORDER BY start_year;

```
---

### `team.bronze.partner_category_map` (SQL, static)

**Purpose:** Maps the literal section-header label found in a workbook's Total Sheet (column 1 of each section's header row) to a canonical partner_category.

- Generated via a one-time `CREATE OR REFRESH TABLE ... AS` SQL statement, not a pipeline table
- Logic: Matching is case-insensitive and whitespace-trimmed: store labels in whatever case is natural, the pipeline normalises both sides.
- Static reference data — not expected to change on a regular cadence

```sql

CREATE TABLE IF NOT EXISTS team.bronze.partner_category_map (
    section_label     STRING  COMMENT 'Literal label as it appears in the Total Sheet section header, column 1',
    partner_category  STRING  COMMENT 'Canonical category: TV, SPONSOR, LICENSING, SUPPLIERS',
    notes             STRING  COMMENT 'Optional: where this variant was first seen'
)
COMMENT 'Hand-maintained mapping of Total Sheet section labels to canonical partner categories. Read by the payment pipeline; never written by it.';

-- Seed with the variants confirmed so far. Safe to re-run: only inserts
-- labels that are not already present.
INSERT INTO team.bronze.partner_category_map (section_label, partner_category, notes)
SELECT * FROM (
    VALUES
        ('BROADCASTERS', 'TV',        'UCL 13-14, UCL 2010-2011, UCL 06-07'),
        ('BROADCASTER',  'TV',        'singular variant, defensive'),
        ('SPONSOR',      'SPONSOR',   'UCL 13-14'),
        ('SPONSORS',     'SPONSOR',   'plural variant'),
        ('SUPPLIERS',    'SUPPLIERS', 'UCL 13-14'),
        ('SUPPLIER',     'SUPPLIERS', 'singular variant, defensive'),
        ('LICENSEE',     'LICENSING', 'UCL 13-14'),
        ('LICENSEES',    'LICENSING', 'plural variant'),
        ('LICENSING',    'LICENSING', 'defensive')
) AS seed(section_label, partner_category, notes)
WHERE NOT EXISTS (
    SELECT 1 FROM team.bronze.partner_category_map m
    WHERE upper(trim(m.section_label)) = upper(trim(seed.section_label))
);
```
### `team.dim.umcc.partner` (SQL, static)

#### A) Creation

**Purpose:** Decision-support query for a human curating umcc_partners
- Not part of the pipeline's runtime logic — its output is meant to be eyeballed, not consumed by any downstream table.

Logic
1) Inventory — every distinct (partner, partner_category) pair actually seen in bronze.payment, with season/country context pulled in from total_sheet_partners (correctly scoped to _source_file, matching the drift/collision caveats already documented in the bronze layer) purely as reference to help a human judge whether two names are the same entity.

2) Whitespace/formatting duplicates — normalizes each name (trim, collapse spaces, lowercase) and surfaces any raw value that maps to more than one literal string, catching the easy cases like double spaces or trailing whitespace.
   
3) Legitimate multi-category partners — names that genuinely span more than one partner_category (the adidas-as-sponsor/supplier/licensee kind of case already known from the bronze brief), flagged so they're deliberately kept as separate dimension rows rather than mistakenly collapsed into one.
   
4) Fuzzy near-duplicates — pairs of names within the same category that are either a prefix of each other or within edit-distance 3 (the "HRT" vs "HRT TV" case), shown side-by-side with their country variants so a human can judge same-partner-different-spelling vs. genuinely-different-partner-that-looks-similar.

```sql

-- 1st exploration

SELECT
    p.partner,
    p.partner_category,
    COLLECT_SET(tsp.season)          AS season_variants,
    COLLECT_SET(tsp.partner_country) AS country_variants,
    COUNT(*)                         AS row_count,
    COUNT(DISTINCT p._source_file)   AS file_count
FROM team.bronze.payment AS p
LEFT JOIN team.bronze.total_sheet_partners AS tsp
    ON  p._source_file = tsp._source_file
    AND UPPER(TRIM(p.partner)) = UPPER(TRIM(tsp.partner_name))
GROUP BY p.partner, p.partner_category
ORDER BY p.partner;

-- 2nd pass optional

SELECT
    LOWER(TRIM(REGEXP_REPLACE(partner, '\\s+', ' '))) AS normalised_key,
    COLLECT_SET(partner)                              AS raw_variants,
    COUNT(DISTINCT partner)                            AS variant_count
FROM team.bronze.payment
GROUP BY LOWER(TRIM(REGEXP_REPLACE(partner, '\\s+', ' ')))
HAVING COUNT(DISTINCT partner) > 1
ORDER BY variant_count DESC;

-- third pass

SELECT
    partner,
    COLLECT_SET(partner_category)    AS categories,
    COUNT(DISTINCT partner_category) AS category_count
FROM team.bronze.payment
GROUP BY partner
HAVING COUNT(DISTINCT partner_category) > 1
ORDER BY partner;

-- Fourth pass

WITH partner_summary AS (
    SELECT
        p.partner,
        p.partner_category,
        COLLECT_SET(tsp.partner_country) AS countries
    FROM team.bronze.payment AS p
    LEFT JOIN team.bronze.total_sheet_partners AS tsp
        ON  p._source_file = tsp._source_file
        AND UPPER(TRIM(p.partner)) = UPPER(TRIM(tsp.partner_name))
    GROUP BY p.partner, p.partner_category
)
SELECT
    a.partner        AS partner_a,
    a.countries       AS countries_a,
    b.partner        AS partner_b,
    b.countries       AS countries_b,
    a.partner_category
FROM partner_summary AS a
JOIN partner_summary AS b
    ON  a.partner_category = b.partner_category
    AND a.partner < b.partner  -- each pair shown once, no self-matches
    AND (
        UPPER(a.partner) LIKE CONCAT(UPPER(b.partner), '%')
        OR UPPER(b.partner) LIKE CONCAT(UPPER(a.partner), '%')
        OR LEVENSHTEIN(UPPER(a.partner), UPPER(b.partner)) <= 3
    )
ORDER BY a.partner_category, a.partner;
```
---

#### B) Add values

```sql

CREATE TABLE IF NOT EXISTS team.dim.umcc_partners (
    partner         STRING NOT NULL COMMENT 'Exact value as it appears in bronze.payment.partner',
    partner_cleaned STRING NOT NULL COMMENT 'Canonical name to use from silver onward, season/category independent',
    CONSTRAINT umcc_partners_pk PRIMARY KEY (partner)
)
COMMENT 'Hand-maintained UMCC partner dimension. Extend whenever the has_partner_cleaned expectation in silver.payment flags an unmapped partner.';

INSERT INTO team.dim.umcc_partners (partner, partner_cleaned) VALUES
    ('(Otisa) Meridiano TV', '(Otisa) Meridiano TV'),
    ('(Otisa)Meridiano', '(Otisa) Meridiano TV'),
    ('365 Media', '365 Media'),
    ('A1 Bulgaria', 'A1 Bulgaria'),
    ('ACE Mexico', 'ACE Mexico'),
    ('ADJARA.COM', 'ADJARA.COM'),
    ('ALTEREGO MASS', 'ALTEREGO MASS'),
    ('AMC Network', 'AMC Networks'),
    ('AMC Networks', 'AMC Networks'),
    ('Al Jazeera', 'Al Jazeera'),
    ('Al Jazeera Sport Channel', 'Al Jazeera'),
    ('Alibaba Culture', 'Alibaba Culture'),
    ('Am Ball Com', 'Am Ball Com GmbH'),
    ('Am Ball Com GmbH', 'Am Ball Com GmbH'),
    ('Amazon Content', 'Amazon Content'),
    ('Antenna TV SA', 'Antenna TV SA'),
    ('Arab Radio', 'Arab Radio'),
    ('Arena Channels', 'Arena Channels'),
    ('Arena Imaging', 'Arena Imaging'),
    ('Armenia TV CJSC', 'Armenia TV CJSC'),
    ('Artmotion', 'Artmotion'),
    ('Astro Arena', 'Astro Arena'),
    ('Azerbaijan TV & Radio', 'Azerbaijan TV & Radio'),
    ('Azerbaijan TV&Radio', 'Azerbaijan TV & Radio'),
    ('BBC', 'BBC'),
    ('BBC Radio 5 Live', 'BBC'),
    ('BHRT', 'BHRT'),
    ('BeTV', 'BeTV'),
    ('Beijing Kuaushou Techn.', 'Beijing Kuaushou Techn.'),
    ('Beijing Sina', 'Beijing Sina'),
    ('Beijing Sina Internet', 'Beijing Sina'),
    ('Beijing XIN''AI Sports', 'Beijing XIN''AI Sports'),
    ('Belarus TV&Radio', 'Belarus TV&Radio'),
    ('British Broadcasting', 'British Broadcasting'),
    ('British Sky BC', 'British Sky BC'),
    ('British Telecom', 'British Telecom'),
    ('Bulgarian National TV', 'Bulgarian National TV'),
    ('C More Entertainment', 'C More Entertainment'),
    ('CBS Broadcasting', 'CBS Broadcasting'),
    ('CCTV', 'CCTV'),
    ('CET 21', 'CET 21'),
    ('CLT- UFA', 'CLT- UFA'),
    ('CLT-UFA', 'CLT- UFA'),
    ('CLT-UFA (Tvi)', 'CLT- UFA'),
    ('CME Programming', 'CME Programming'),
    ('CT Cinetrade', 'CT Cinetrade AG'),
    ('CT Cinetrade AG', 'CT Cinetrade AG'),
    ('Cable&Wireless', 'Cable&Wireless'),
    ('Cableurope', 'Cableurope'),
    ('Cadena Cope', 'Cadena Cope'),
    ('Canal Dos Corp.', 'Canal Dos Corp.'),
    ('Canal+', 'Canal+'),
    ('Canal+ Antilles', 'Canal+'),
    ('Canal+ Myanmar', 'Canal+'),
    ('Caracol', 'Caracol'),
    ('Century Sun Intern.', 'Century Sun Intern.'),
    ('Channel One', 'Channel One'),
    ('Channel One Company', 'Channel One'),
    ('Charlton Ltd.', 'Charlton Ltd.'),
    ('Chello Central', 'Chello Central'),
    ('Chellomedia Direct Progr.', 'Chellomedia Direct Progr.'),
    ('China Media Group', 'China Media Group'),
    ('Cineplex Co. Ltd.', 'Cineplex Co. Ltd.'),
    ('Clever Media', 'Clever Media'),
    ('Commercial TV Channel', 'Commercial TV Channel'),
    ('Continental TV', 'Continental TV'),
    ('Corp. Radiotel. - RNE', 'Corp. Radiotel. - RNE'),
    ('Corporacion Medcom', 'Corporacion Medcom'),
    ('Creative Programs', 'Creative Programs'),
    ('Cyprus Broadcasting', 'Cyprus Broadcasting'),
    ('Cyprus Telecomm.', 'Cyprus Telecomm.'),
    ('Czech TV', 'Czech TV'),
    ('DAZN', 'DAZN'),
    ('DAZN Limited', 'DAZN'),
    ('DCA Sydney', 'DCA Sydney'),
    ('DIGITALB SH.A.', 'DIGITALB SH.A.'),
    ('DPG Media NV', 'DPG Media NV'),
    ('DR', 'DR'),
    ('DTS - Canal+ SA', 'DTS - Canal+ SA'),
    ('Digicel (PNG) Limtied', 'Digicel (PNG) Limtied'),
    ('Digitalb', 'Digitalb'),
    ('Dogan TV (Star TV)', 'Dogan TV (Star TV)'),
    ('E.TV (PTY) Ltd.', 'E.TV (PTY) Ltd.'),
    ('ECLAT Entertainm.', 'ECLAT Entertainm.'),
    ('ELTA Technology', 'Elta Technology'),
    ('ERT S.A.', 'ERT S.A.'),
    ('ESPN', 'ESPN'),
    ('ESPN (EMEA) Ltd.', 'ESPN'),
    ('ESPN Inc', 'ESPN'),
    ('EXPEDIA', 'Expedia'),
    ('Electronic Arts', 'Electronic Arts'),
    ('Eletronic Arts', 'Electronic Arts'),
    ('Eleven Sports Network', 'Eleven Sports Network'),
    ('Elms Marketing', 'Elms Marketing'),
    ('Elta Technology', 'Elta Technology'),
    ('Elta Tehnology', 'Elta Technology'),
    ('Empresa Metropolitana', 'Empresa Metropolitana'),
    ('Encanto', 'Encanto'),
    ('Engelbert-Strauss', 'Engelbert-Strauss'),
    ('Entain Operations Ltd', 'Entain Operations Ltd'),
    ('Enterprise', 'Enterprise'),
    ('Eredivisie Media&Marketing', 'Eredivisie Media&Marketing'),
    ('Eurasian Broadcasting', 'Eurasian Broadcasting'),
    ('Europe 1', 'Europe 1'),
    ('Eurosport', 'Eurosport'),
    ('Event Merchandise Ltd.', 'Event Merchandise Ltd.'),
    ('FORD', 'Ford'),
    ('FOX Pan American', 'FOX Pan American'),
    ('FPT Telecom', 'FPT Telecom'),
    ('Fedex', 'Fedex'),
    ('Forthnet', 'Forthnet'),
    ('Fox Pan American', 'Fox Pan American'),
    ('France Medias Monde', 'France Medias Monde'),
    ('Frito-Lay Trading', 'Frito-Lay Trading'),
    ('Fuji Television', 'Fuji Television'),
    ('GIBTELECOM', 'GIBTELECOM'),
    ('GLOBO Comm.', 'GLOBO Comm.'),
    ('GO P.L.C', 'GO P.L.C'),
    ('GOL TV-Mediaproduccion', 'GOL TV-Mediaproduccion'),
    ('Gazprom', 'Gazprom'),
    ('Georgian Public BC', 'Georgian Public BC'),
    ('Gibraltar Broadcasting', 'Gibraltar Broadcasting'),
    ('Global Media Group', 'Global Media Group'),
    ('Great Sports Media', 'Great Sports Media'),
    ('Groupe Canal plus', 'Canal+'),
    ('Groupe Canal+', 'Canal+'),
    ('Grupo Megavision', 'Grupo Megavision'),
    ('HIT Radio', 'HIT Radio'),
    ('HRT', 'HRTV'),
    ('HRTV', 'HRTV'),
    ('HTC', 'HTC'),
    ('Hankook', 'Hankook Tire'),
    ('Hankook Tire', 'Hankook Tire'),
    ('Heineken', 'Heineken'),
    ('Hrvatski Telekom', 'Hrvatski Telekom'),
    ('Hublot SA', 'Hublot SA'),
    ('Hungarian TV', 'Hungarian TV'),
    ('Hy-Pro Intern.', 'Hy-Pro Intern.'),
    ('ICS Prime TV SRL', 'ICS Prime TV SRL'),
    ('IMG Media', 'IMG Media'),
    ('ITI Neovision', 'ITI Neovision'),
    ('ITV', 'ITV'),
    ('ITW', 'ITW'),
    ('Icons Shop Ltd.', 'Icons Shop Ltd.'),
    ('InfoNetwork', 'InfoNetwork'),
    ('Infostrada', 'Infostrada'),
    ('Intern. Media Content', 'Intern. Media Content'),
    ('Intern.Media Content', 'Intern. Media Content'),
    ('Jacques Lemans GmbH', 'Jacques Lemans GmbH'),
    ('Konami', 'Konami'),
    ('Liberty Global', 'Liberty Global'),
    ('Longshore Limited', 'Longshore Limited'),
    ('MEGOGO', 'MEGOGO'),
    ('MHC Marketing', 'MHC Marketing'),
    ('MKRTV', 'MKRTV'),
    ('MTEL', 'MTEL'),
    ('MTRK', 'MTRK'),
    ('MTV Oy', 'MTV Oy'),
    ('MTVA', 'MTVA'),
    ('Makedonski Telekom', 'Makedonski Telekom'),
    ('MasterCard', 'MasterCard'),
    ('Media 6 SH.A (TV Klan)', 'Media 6 SH.A (TV Klan)'),
    ('Media Service Support', 'Media Service Support'),
    ('Mediapro', 'Mediapro'),
    ('Mediaset/Telecinco', 'Mediaset/Telecinco'),
    ('Melita Cable', 'Melita Cable'),
    ('Melita plc.', 'Melita plc.'),
    ('Metropole M6', 'Metropole M6'),
    ('Metropole TV', 'Metropole TV'),
    ('Milan Entertainment SRL', 'Milan Entertainment SRL'),
    ('Molten Corporation', 'Molten Corporation'),
    ('NENT', 'NENT'),
    ('NENT UK', 'NENT'),
    ('NKR', 'NKR'),
    ('NOS', 'NOS'),
    ('NTV Plus', 'NTV Plus'),
    ('National Sports Channel', 'National Sports Channel'),
    ('Netmed Hellas', 'Netmed Hellas'),
    ('Nippon Television', 'Nippon Television'),
    ('Nordic Entertainm.', 'Nordic Entertainment'),
    ('Nordic Entertainment', 'Nordic Entertainment'),
    ('ORF', 'ORF'),
    ('OTE (Hellenic Telecomm. Org)', 'OTE (Hellenic Telecomm. Org)'),
    ('OTI', 'OTI'),
    ('Octane5', 'Octane5'),
    ('On Site Producciones', 'On Site Producciones'),
    ('Only Fans Ltd.', 'Only Fans Ltd.'),
    ('Opsec Security Ltd.', 'Opsec Security Ltd.'),
    ('Ottewill Silversmiths', 'Ottewill Silversmiths'),
    ('PBS', 'PBS'),
    ('PLAYSTATION', 'PLAYSTATION'),
    ('POST Modern', 'POST Modern Edit'),
    ('POST Modern Edit', 'POST Modern Edit'),
    ('PRVA TV d.o.o.', 'PRVA TV d.o.o.'),
    ('PT Surya Citra', 'PT Surya Citra'),
    ('PULS 4 TV', 'PULS 4 TV'),
    ('Panini', 'Panini'),
    ('Polsat', 'Polsat'),
    ('Pro Plus', 'Pro Plus'),
    ('Pro TV', 'Pro TV'),
    ('ProSiebenSat.1', 'ProSiebenSat.1'),
    ('Procter & Gamble', 'Procter & Gamble'),
    ('Proximus', 'Proximus'),
    ('Proyectum Sport', 'Proyectum Sport'),
    ('Public Broadcasting', 'Public Broadcasting'),
    ('RAI', 'RAI'),
    ('RCN Radio', 'RCN Radio'),
    ('RCS&RDS', 'RCS&RDS'),
    ('RTBF', 'RTBF'),
    ('RTE', 'RTE'),
    ('RTI', 'RTI (Mediaset)'),
    ('RTI (Mediaset)', 'RTI (Mediaset)'),
    ('RTL', 'RTL'),
    ('RTL Belux', 'RTL Belux'),
    ('RTP', 'RTP'),
    ('RTS', 'RTS'),
    ('RTS - Serbian BC', 'RTS'),
    ('RTSH', 'RTSH'),
    ('RTV', 'RTV'),
    ('RTV Slovenija', 'RTV'),
    ('Radio Autonomica Madrid', 'Radio Autonomica Madrid'),
    ('Radio Itatiaia', 'Radio Itatiaia'),
    ('Radio Publicidad', 'Radio Publicidad'),
    ('Radio&Television of', 'Radio&Television of'),
    ('Red Bull Media House', 'Red Bull Media House'),
    ('Red de TV', 'Red de TV'),
    ('Rogers Sportsnet', 'Rogers Sportsnet'),
    ('RomTelecom', 'RomTelecom'),
    ('Rustavi 2', 'Rustavi 2'),
    ('S.C. Pro TV', 'S.C. Pro TV'),
    ('S.C. RCS&RDS', 'S.C. RCS&RDS'),
    ('S.C. RCS&RDS S.A.', 'S.C. RCS&RDS'),
    ('SBF Comercio de productos', 'SBF Comercio de productos'),
    ('SBS', 'SBS'),
    ('SIC', 'SIC'),
    ('SOCIOS Services', 'SOCIOS Services'),
    ('SPS HD', 'SPS HD'),
    ('SRG', 'SRG'),
    ('STAN Entertainment', 'STAN Entertainment'),
    ('STVS', 'STVS'),
    ('SYN', 'SYN'),
    ('Sanoma Digital Media', 'Sanoma Digital Media'),
    ('Sanoma Entertainment', 'Sanoma Entertainment'),
    ('Saran Media', 'Saran Media'),
    ('Setanta', 'Setanta'),
    ('Setanta Sport', 'Setanta'),
    ('Shenzhen Tencent', 'Shenzhen Tencent'),
    ('Shwe Than Lwin Media', 'Shwe Than Lwin Media'),
    ('Sigma Radio TV', 'Sigma Radio TV'),
    ('Silknet', 'Silknet'),
    ('SingTel i2i Pte', 'SingTel i2i Pte'),
    ('Sirius XM', 'Sirius XM'),
    ('Sirius XM Radio', 'Sirius XM'),
    ('Sky Austria', 'Sky'),
    ('Sky Austria GmbH', 'Sky'),
    ('Sky Deutschland', 'Sky'),
    ('Sky Italia', 'Sky'),
    ('Sky Italy', 'Sky'),
    ('Sky Perfect', 'Sky Perfect'),
    ('Sky Perfect Comm.', 'Sky Perfect'),
    ('Skynet iMotions', 'Skynet iMotions'),
    ('Slovak TV', 'Slovak TV'),
    ('Slovak Telekom', 'Slovak TV'),
    ('Sociedad Espanola', 'Sociedad Espanola'),
    ('Societat Valenciana', 'Societat Valenciana'),
    ('Sony Pictures', 'Sony Group'),
    ('Sony UK', 'Sony Group'),
    ('Spark New Zealand', 'Spark New Zealand'),
    ('Sport TV', 'Sport TV'),
    ('SportA', 'SportA'),
    ('Sportandem', 'Sportandem'),
    ('Sports Endeavours', 'Sports Endeavours'),
    ('State National TV', 'State National TV'),
    ('State of Football BV', 'State of Football BV'),
    ('Supersport', 'Supersport'),
    ('Surya Citra Televisi', 'Surya Citra Televisi'),
    ('Swissquote', 'Swissquote'),
    ('TAJ TV', 'TAJ TV'),
    ('TAKEAWAY.COM', 'Just Eat Takeaway.com'),
    ('TAP Digital Media', 'TAP Digital Media'),
    ('TDM', 'TDM'),
    ('TF1', 'TF1'),
    ('TRBC Ukraine LLC', 'TRBC Ukraine LLC'),
    ('TV Media Planet Ltd.', 'TV Media Planet Ltd.'),
    ('TV2 AS', 'TV2'),
    ('TV2 Danmark', 'TV2'),
    ('TV3', 'TV3'),
    ('TV3 Television', 'TV3'),
    ('TV4 AB', 'TV4 AB'),
    ('TVC Catalunya', 'TVC Catalunya'),
    ('TVE', 'TVE'),
    ('TVI', 'TVI'),
    ('TVR', 'TVR'),
    ('TVSBT', 'TVSBT'),
    ('TVSBT Canal4 de Sao Paolo', 'TVSBT'),
    ('Talpa', 'Talpa'),
    ('Teleclub AG', 'Teleclub AG'),
    ('Telecorp.Salvadorena', 'Telecorp.Salvadorena'),
    ('Teledifusao de Macau', 'Teledifusao de Macau'),
    ('Telefonica', 'Telefonica'),
    ('Telekom Serbia', 'Telekom'),
    ('Telekom Slovenje', 'Telekom'),
    ('Telemach', 'Telemach'),
    ('Telenet BVBA', 'Telenet BVBA'),
    ('Telenet N.V.', 'Telenet N.V.'),
    ('Teletica', 'Teletica'),
    ('Televicentro', 'Televicentro'),
    ('Televisora Nacional', 'Televisora Nacional'),
    ('Telia Company', 'Telia Company'),
    ('Terra Networks', 'Terra Networks'),
    ('The Great Branding Co.', 'The Great Branding Co.'),
    ('The Phoenix Luxury', 'The Phoenix Luxury'),
    ('The Sports Channel Ltd.', 'The Sports Channel Ltd.'),
    ('The Walt Disney', 'The Walt Disney'),
    ('TopSports Ventures', 'TopSports Ventures'),
    ('Topps Company Ltd.', 'Topps'),
    ('Topps Europe Ltd.', 'Topps'),
    ('Topsports Ventures', 'Topsports Ventures'),
    ('Tring TV', 'Tring TV'),
    ('Turner Intern.', 'Turner Intern.'),
    ('TwelfthMan', 'TwelfthMan'),
    ('UML - United Media', 'UML - United Media'),
    ('UML - United Media Ltd.', 'UML - United Media'),
    ('Unedisa Comuncaciones', 'Unedisa Comunicaciones'),
    ('Unedisa Comunicac', 'Unedisa Comunicaciones'),
    ('Unedisa Comunicaciones', 'Unedisa Comunicaciones'),
    ('UniCredit', 'UniCredit'),
    ('Uniprex', 'Uniprex SA'),
    ('Uniprex SA', 'Uniprex SA'),
    ('Univision', 'Univision'),
    ('Utd. Football BC', 'Utd. Football BC'),
    ('VOO S.A. (ex beTV)', 'VOO S.A. (ex beTV)'),
    ('VSTV', 'VSTV'),
    ('VTV', 'VTV'),
    ('Viasat / TV3', 'Viasat'),
    ('Viasat BC UK', 'Viasat'),
    ('Viasat Sport A/S', 'Viasat'),
    ('Viasat UK', 'Viasat'),
    ('Vivaro Media LLC', 'Vivaro Media LLC'),
    ('Vlaamse Media', 'Vlaamse Media'),
    ('Vlaamse Radio', 'Vlaamse Media'),
    ('Vodafone', 'Vodafone'),
    ('WOWOW', 'WOWOW'),
    ('Western Union', 'Western Union'),
    ('Yleisradio', 'Yleisradio'),
    ('ZDF', 'ZDF'),
    ('adidas', 'adidas'),
    ('bTV Media', 'bTV Media Group'),
    ('bTV Media Group', 'bTV Media Group'),
    ('beIN Asia', 'beIN'),
    ('beIN IP Ltd.', 'beIN'),
    ('beIN Sports France', 'beIN'),
    ('beIN Sports Media', 'beIN'),
    ('de la Rue Intern.', 'de la Rue Intern.'),
    ('i-Cable Sports Ltd.', 'i-Cable Sports Ltd.'),
    ('talkSport', 'talkSport'),
    ('talksport', 'talkSport');

```
