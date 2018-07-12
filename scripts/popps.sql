-- VIEW: boundaries
-- Connects all three admin levels together
CREATE OR REPLACE VIEW boundaries AS
SELECT
    ward.id,
    ward.globalid,
    ward.timestamp,
    ward.amapcode,
    ward.source,
    ward.urban,
    ward.wardcode as "ward_code",
    ward.wardname as "ward_name",
    ward.lgacode as "lga_code",
    lga.lganame as "lga_name",
    lga.statecode as "state_code",
    state.statename as "state_name",
    ward.geom as "geom_ward",
    lga.geom as "geom_lga",
    state.geom as "geom_state"
FROM boundary_vaccwards as ward
JOIN boundary_vacclgas as lga
  ON ward.lgacode = lga.lgacode
JOIN boundary_vaccstates as state
  ON lga.statecode = state.statecode;


-- VIEW: fe_settlements
-- Collects all settlements together
CREATE OR REPLACE VIEW fe_settlements AS
SELECT 
    id, globalid, timestamp, wardcode, 'BUA' as settlementtype,
    settlementid, settlementname,
    building_assessment_intact,
    building_assessment_inhabited,
    building_assessment_confidence,
    building_assessment_date,
    settlementobjectid, settlementnamecalc,
    weight, warddistance, lgadistance,
    majorroaddistance, tertiaryroaddistance,
    nbdenominator, geom
FROM fe_builtuparea
UNION SELECT
    id, globalid, timestamp, wardcode, 'SSA',
    settlementid, settlementname,
    building_assessment_intact,
    building_assessment_inhabited,
    building_assessment_confidence,
    building_assessment_date,
    settlementobjectid, settlementnamecalc,
    weight, warddistance, lgadistance,
    majorroaddistance, tertiaryroaddistance,
    nbdenominator, geom
FROM fe_smlsettlementareas
UNION SELECT
    id, globalid, timestamp, wardcode, 'HA',
    settlementid, settlementname,
    building_assessment_intact,
    building_assessment_inhabited,
    building_assessment_confidence,
    building_assessment_date,
    settlementobjectid, settlementnamecalc,
    weight, warddistance, lgadistance,
    majorroaddistance, tertiaryroaddistance,
    nbdenominator, geom
FROM fe_hamletareas


-- FUNCTION: getWardPopSummary
CREATE OR REPLACE FUNCTION getWardPopSummary (ageFrom INTEGER, ageTo INTEGER)
RETURN TABLE (
    globalid VARCHAR,
    wardcode VARCHAR,
    popvalue FLOAT
) AS $$
DECLARE
    sourceTarget VARCHAR := 'Worldpop / ORNL NOT Adjusted'
BEGIN
    SELECT
        globalid,
        wardcode,
        (SUM(CASE WHEN gender='M' THEN value ELSE END) +
         SUM(CASE WHEN gender='F' THEN value ELSE END)
        ) as "popvalue"
    FROM vts_population_estimate
    WHERE age_group_from = ageFROM
      AND age_group_to = ageTo
      AND source = sourceTarget
    GROUP BY (globalid, featureidentifier, wardcode)
END; $$
LANGUAGE 'plpgsql'

-- FUNTION: getZoneWards
-- Returns list of Ward code for the specified geo-political zone
CREATE OR REPLACE FUNCTION getZoneWards (zoneName VARCHAR)
RETURN VARCHAR ARRAY AS $$
DECLARE
    ZONE_NE VARCHAR ARRAY := ARRAY['AD', 'BA', 'BO', 'GO', 'TA', 'YO'];
    ZONE_NC VARCHAR ARRAY := ARRAY['BE', 'KO', 'KW', 'NA', 'NI', 'PL', 'FC'];
    ZONE_NW VARCHAR ARRAY := ARRAY['JI', 'KD', 'KN', 'KT', 'KB', 'SO', 'ZM'];
    ZONE_SW VARCHAR ARRAY := ARRAY['EK', 'LG', 'OG', 'ON', 'OS', 'OY']
    ZONE_SS VARCHAR ARRAY := ARRAY['AI', 'CR', 'BA', 'RI', 'DE', 'ED'];
    ZONE_SE VARCHAR ARRAY := ARRAY['AB', 'AN', 'EB', 'EN', 'IM'];
    ZONE_RT VARCHAR ARRAY;
BEGIN
    IF zoneName IN ['NE', 'NC', 'NW', 'SW', 'SS', 'SE'] THEN
        EXECUTE (
            'SELECT ward_code FROM boundaries WHERE state_code IN ZONE_' ||
            zoneName  || ';' INTO ZONE_RT
        );
        RETURN ZONE_RT;
    END IF
    RETURN [];
END; $$
LANGUAGE 'plpgsql'


-- VIEW: settlement_pop
CREATE OR REPLACE VIEW settlement_pop AS
SELECT
    tbl1_4.globalid,
    tbl1_4.wardcode,
    tbl1_4.popvalue as "pop1_4",
    tbl5_9.popvalue as "pop5_9",
    tbl10_14.popvalue as "pop10_14",
    tbl15_19.popvalue as "pop15_19",
    tbl20_24.popvalue as "pop20_24",
    tbl25_29.popvalue as "pop25_29",
    tbl30_34.popvalue as "pop30_34",
    tbl35_49.popvalue as "pop35_39",
    tbl40_44.popvalue as "pop40_44",
    tbl45_49.popvalue as "pop45_49",
    tbl50_54.popvalue as "pop50_54",
    tbl55_59.popvalue as "pop55_59",
    tbl60_64.popvalue as "pop60_64",
    tbl65_69.popvalue as "pop65_69",
    tbl70_74.popvalue as "pop70_74",
    tbl75_100.popvalue as "pop75_100",
    (COALESCE(tbl1_4.popvalue, 0.0)   + COALESCE(tbl5_9.popvalue, 0.0) +
     COALESCE(tbl10_14.popvalue, 0.0) + COALESCE(tbl15_19.popvalue, 0.0) +
     COALESCE(tbl20_24.popvalue, 0.0) + COALESCE(tbl25_29.popvalue, 0.0) +
     COALESCE(tbl30_34.popvalue, 0.0) + COALESCE(tbl35_39.popvalue, 0.0) +
     COALESCE(tbl40_44.popvalue, 0.0) + COALESCE(tbl45_49.popvalue, 0.0) +
     COALESCE(tbl50_54.popvalue, 0.0) + COALESCE(tbl55_59.popvalue, 0.0) +
     COALESCE(tbl60_64.popvalue, 0.0) + COALESCE(tbl65_69.popvalue, 0.0) +
     COALESCE(tbl70_74.popvalue, 0.0) + COALESCE(tbl75_100.popvalue, 0.0)
    ) as "pop_total"
FROM getWardPopSummary(0, 4) as tbl1_4
LEFT JOIN getWardPopSummary(5, 9) as tbl5_9
  ON tbl1_4.globalid = tbl5_9.globalid
LEFT JOIN getWardPopSummary(10, 14) as tbl10_14
  ON tbl5_9.globalid = tbl10_14.globalid
LEFT JOIN getWardPopSummary(15, 19) as tbl15_19
  ON tbl10_14.globalid = tbl15_19.globalid

LEFT JOIN getWardPopSummary(20, 24) as tbl20_24
  ON tbl15_19.globalid = tbl20_24.globalid
LEFT JOIN getWardPopSummary(25, 29) as tbl25_29
  ON tbl20_24.globalid = tbl25_29.globalid

LEFT JOIN getWardPopSummary(30, 34) as tbl30_34
  ON tbl25_29.globalid = tbl30_34.globalid
LEFT JOIN getWardPopSummary(35, 39) as tbl35_39
  ON tbl30_34.globalid = tbl35_39.globalid

LEFT JOIN getWardPopSummary(40, 44) as tbl40_44
  ON tbl35_39.globalid = tbl40_44.globalid
LEFT JOIN getWardPopSummary(45, 49) as tbl45_49
  ON tbl40_44.globalid = tbl45_49.globalid

LEFT JOIN getWardPopSummary(50, 54) as tbl50_54
  ON tbl45_49.globalid = tbl50_54.globalid
LEFT JOIN getWardPopSummary(55, 59) as tbl55_59
  ON tbl50_54.globalid = tbl55_59.globalid

LEFT JOIN getWardPopSummary(60, 64) as tbl60_64
  ON tbl55_59.globalid = tbl60_64.globalid
LEFT JOIN getWardPopSummary(65, 69) as tbl65_69
  ON tbl60_64.globalid = tbl65_69.globalid

LEFT JOIN getWardPopSummary(70, 74) as tbl70_74
  ON tbl65_69.globalid = tbl70_74.globalid
LEFT JOIN getWardPopSummary(75, 100) as tbl75_100
  ON tbl70_74.globalid = tbl75_100.globalid;


-- FUNCTON: getSettlementPopByZone
CREATE OR REPLACE FUNCTION getSettlementPopByZone(zoneName VARCHAR)
RETURN TABLE (
    globalid VARCHAR, settlement_name VARCHAR, ward_code VARCHAR,
    pop1_4 FLOAT,   pop5_9 FLOAT,   pop10_14 FLOAT, pop15_19 FLOAT,
    pop20_24 FLOAT, pop25_29 FLOAT, pop30_34 FLOAT, pop35_39 FLOAT,
    pop40_44 FLOAT, pop45_49 FLOAT, pop50_54 FLOAT, pop55_59 FLOAT,
    pop60_64 FLOAT, pop65_69 FLOAT, pop70_100 FLOAT, pop_total FLOAT,
    geom GEOMETRY
) AS $$
BEGIN
    SELECT 
        fe.globalid, fe.settlement_name, fe.ward_code,
        pp.age1_4,   pp.age5_9,   pp.age10_14, pp.age15_19,
        pp.age20_24, pp.age25_29, pp.age30_34, pp.age35_39,
    FROM settlement_pop as pp
    JOIN fe_settlements as fe
      ON pp.globalid = fe.globalid
    WHERE pp.ward_code IN getZoneWards(zoneName)
END; $$
LANGUAGE 'plpgsql';


-- FUNCTION: 
CREATE OR REPLACE FUNCTION buildPopTables()
AS $$
DECLARE
    ZONES VARCHAR ARRAY := ARRAY['NE', 'NC', 'NW', 'SW', 'SS', 'SE']
BEGIN



END; $$
LANGUAGE 'plpgsql'