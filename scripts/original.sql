CREATE VIEW settlement_pop AS 
 SELECT one_four1.globalid,
    one_four1.wardcode,
    one_four1.one_four,
    five_nine1.five_nine,
    ten_fourteen1.ten_fourteen,
    fifteen_nineteen1.fifteen_nineteen,
    twenty_twentyfour1.twenty_twentyfour,
    twentyfive_twentynine1.twentyfive_twentynine,
    thirty_thirtyfour1.thirty_thirtyfour,
    thirtyfive_thirtynine1.thirtyfive_thirtynine,
    forty_fortyfour1.forty_fortyfour,
    fortyfour_fortynine1.fortyfive_fortynine,
    fifty_fiftyfour1.fifty_fiftyfour,
    fiftyfive_fiftynine1.fiftyfive_fiftynine,
    sixty_sityfour1.sixty_sixtyfour,
    sixtyfive_sixtynine1.sixtyfive_sixtynine,
    seventy_seventyfour1.seventy_seventyfour,
    seventyfive_hundred1.seventyfive_hundred,
    COALESCE(one_four1.one_four, ) + COALESCE(five_nine1.five_nine, ) + COALESCE(ten_fourteen1.ten_fourteen, ) + COALESCE(fifteen_nineteen1.fifteen_nineteen, ) + COALESCE(twenty_twentyfour1.twenty_twentyfour, ) + COALESCE(twentyfive_twentynine1.twentyfive_twentynine, ) + COALESCE(thirty_thirtyfour1.thirty_thirtyfour, ) + COALESCE(thirtyfive_thirtynine1.thirtyfive_thirtynine, ) + COALESCE(forty_fortyfour1.forty_fortyfour, ) + COALESCE(fortyfour_fortynine1.fortyfive_fortynine, ) + COALESCE(fifty_fiftyfour1.fifty_fiftyfour, ) + COALESCE(fiftyfive_fiftynine1.fiftyfive_fiftynine, ) + COALESCE(sixty_sityfour1.sixty_sixtyfour, ) + COALESCE(sixtyfive_sixtynine1.sixtyfive_sixtynine, ) + COALESCE(seventy_seventyfour1.seventy_seventyfour, ) + COALESCE(seventyfive_hundred1.seventyfive_hundred, ) AS total_pp
   FROM ( SELECT globalid::uuid AS globalid,wardcode,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS one_four
           FROM vts_populationestimate
          WHERE age_group_to <= 4 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) one_four1
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS five_nine
           FROM vts_populationestimate
          WHERE age_group_from = 5 AND age_group_to = 9 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) five_nine1 ON one_four1.globalid = five_nine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS ten_fourteen
           FROM vts_populationestimate
          WHERE age_group_from = 10 AND age_group_to = 14 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) ten_fourteen1 ON five_nine1.globalid = ten_fourteen1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS fifteen_nineteen
           FROM vts_populationestimate
          WHERE age_group_from = 15 AND age_group_to = 19 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) fifteen_nineteen1 ON ten_fourteen1.globalid = fifteen_nineteen1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS twenty_twentyfour
           FROM vts_populationestimate
          WHERE age_group_from = 20 AND age_group_to = 24 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) twenty_twentyfour1 ON fifteen_nineteen1.globalid = twenty_twentyfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS twentyfive_twentynine
           FROM vts_populationestimate
          WHERE age_group_from = 25 AND age_group_to = 29 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) twentyfive_twentynine1 ON twentyfive_twentynine1.globalid = twenty_twentyfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS thirty_thirtyfour
           FROM vts_populationestimate
          WHERE age_group_from = 30 AND age_group_to = 34 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) thirty_thirtyfour1 ON twentyfive_twentynine1.globalid = thirty_thirtyfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS thirtyfive_thirtynine
           FROM vts_populationestimate
          WHERE age_group_from = 35 AND age_group_to = 39 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) thirtyfive_thirtynine1 ON thirtyfive_thirtynine1.globalid = thirty_thirtyfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS forty_fortyfour
           FROM vts_populationestimate
          WHERE age_group_from = 40 AND age_group_to = 44 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) forty_fortyfour1 ON forty_fortyfour1.globalid = thirtyfive_thirtynine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS fortyfive_fortynine
           FROM vts_populationestimate
          WHERE age_group_from = 45 AND age_group_to = 49 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) fortyfour_fortynine1 ON fortyfour_fortynine1.globalid = forty_fortyfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS fifty_fiftyfour
           FROM vts_populationestimate
          WHERE age_group_from = 50 AND age_group_to = 54 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) fifty_fiftyfour1 ON fifty_fiftyfour1.globalid = fortyfour_fortynine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS fiftyfive_fiftynine
           FROM vts_populationestimate
          WHERE age_group_from = 55 AND age_group_to = 59 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) fiftyfive_fiftynine1 ON fifty_fiftyfour1.globalid = fiftyfive_fiftynine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS sixty_sixtyfour
           FROM vts_populationestimate
          WHERE age_group_from = 60 AND age_group_to = 64 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) sixty_sityfour1 ON sixty_sityfour1.globalid = fiftyfive_fiftynine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS sixtyfive_sixtynine
           FROM vts_populationestimate
          WHERE age_group_from = 65 AND age_group_to = 69 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) sixtyfive_sixtynine1 ON sixtyfive_sixtynine1.globalid = sixty_sityfour1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS seventy_seventyfour
           FROM vts_populationestimate
          WHERE age_group_from = 70 AND age_group_to = 74 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) seventy_seventyfour1 ON seventy_seventyfour1.globalid = sixtyfive_sixtynine1.globalid
     LEFT JOIN ( SELECT globalid::uuid AS globalid,
            sum(
                CASE
                    WHEN gender = 'F' THEN value
                    ELSE 
                END) + sum(
                CASE
                    WHEN gender = 'M' THEN value
                    ELSE 
                END) AS seventyfive_hundred
           FROM vts_populationestimate
          WHERE age_group_from = 75 AND age_group_to = 100 AND source = 'Worldpop / ORNL NOT Adjusted'
          GROUP BY globalid, featureidentifier, wardcode) seventyfive_hundred1 ON seventy_seventyfour1.globalid = seventyfive_hundred1.globalid;

ALTER TABLE public.pp_final
  OWNER TO gridgeoserver;.vts_populationestimates;
  
 CREATE VIEW fe_settlements AS 
 	SELECT "grid_bound.fe_smlsettlementareas2".geom,
    "grid_bound.fe_smlsettlementareas2".settlementid,
    "grid_bound.fe_smlsettlementareas2".settlementname,
    "grid_bound.fe_smlsettlementareas2".wardcode,
    "grid_bound.fe_smlsettlementareas2".globalid
   FROM grid_bound."grid_bound.fe_smlsettlementareas2"
UNION
 SELECT "grid_bound.fe_hamletareas2".geom,
    "grid_bound.fe_hamletareas2".settlementid,
    "grid_bound.fe_hamletareas2".settlementname,
    "grid_bound.fe_hamletareas2".wardcode,
    "grid_bound.fe_hamletareas2".globalid
   FROM grid_bound."grid_bound.fe_hamletareas2"
UNION
 SELECT "grid_bound.fe_builtuparea2".geom,
    "grid_bound.fe_builtuparea2".settlementid,
    "grid_bound.fe_builtuparea2".settlementname,
    "grid_bound.fe_builtuparea2".wardcode,
    "grid_bound.fe_builtuparea2".globalid
   FROM grid_bound."grid_bound.fe_builtuparea2";

ALTER TABLE grid_bound.fe_pp_all2
  OWNER TO postgres;
  
 CREATE VIEW northeast_pop_settlement AS 
 	SELECT pp_final_woa.globalid,
    fe_settlements.settlementname,
    fe_settlements.wardcode,
    one_four,
    five_nine,
    ten_fourteen,
    fifteen_nineteen,
    twenty_twentyfour,
    twentyfive_twentynine,
    thirty_thirtyfour,
    thirtyfive_thirtynine,
    forty_fortyfour,
    fortyfive_fortynine,
    fifty_fiftyfour,
    fiftyfive_fiftynine,
    sixty_sixtyfour,
    sixtyfive_sixtynine,
    seventy_seventyfour,
    seventyfive_hundred,
    total_pp,
    fe_settlement.geom
   FROM settlement_pop
     JOIN fe_settlements ON pp_final_woa.globalid = fe_pp.globalid
  WHERE pp_final_woa.wardcode = ANY (ARRAY['70811', 'TRSTTM10', 'GMSGME03', 'GMSBLR10', 'ADSGAN03', '70311', 'TRSDGA04', 'ADSMAH08', 'GMSFKY03', 'TRSUSS06', 'TRSARD07', 'GMSGME08', 'TRSWKR04', 'GMSKLT05', 'GMSKLT09', 'ADSGYL03', 'BA0307', 'BA1607', 'BA0301', 'BA1912', 'BA1905', 'BA0911', 'BA2002', 'GMSKWM09', 'TRSGAS11', '71303', '71109', '71010', 'GMSYDB11', 'TRSSDA09', '12108', 'GMSFKY05', 'TRSDGA02', 'GMSSHM03', 'TRSWKR06', 'BA1605', 'GMSDKU11', '12407', 'GMSBLR05', 'GMSBLR02', 'GMSBLR03', 'GMSGME04', 'GMSGME10', 'GMSGME05', 'BA0713', 'BA0602', 'TRSARD05', 'GMSGME07', 'GMSKLT08', 'BA1610', '71707', 'BA1409', 'BA0708', '10105', '71704', 'TRSDGA08', '10108', 'TRSSDA07', 'TRSSDA06', 'GMSKLT07', 'GMSKLT10', 'GMSKLT04', 'GMSKLT03', 'TRSUSS04', 'TRSKRM08', 'GMSNFD08', 'GMSSHM02', 'ADSHNG04', 'TRSUSS01', 'ADSYLA07', 'TRSUSS10', 'TRSSDA08', 'TRSTTM09', 'GMSSHM05', 'GMSSHM06', 'ADSHNG10', '12109', 'GMSSHM04', '16006_01', '12104', 'BA1615', '70810', 'TRSSDA04', 'TRSDGA05', 'TRSDGA06', 'GMSKWM08', 'TRSKRM01', '12510', 'TRSARD02', 'GMSDKU03', 'TRSARD10', 'GMSKWM02', '11803', '12107', 'TRSARD08', 'TRSARD01', 'TRSARD09', 'ADSYLN06', 'BA0111', '12103', 'TRSSDA05', 'TRSARD06', 'GMSNFD02', 'TRSKLD08', '10709', 'GMSYDB07', 'GMSFKY04', 'ADSMAH09', 'TRSKLD02', 'TRSARD04', 'GMSNFD04', 'ADSGYL01', 'TRSBBB06', 'GMSBLG06', 'GMSAKK02', 'GMSSHM10', 'BA1603', 'TRSBBB01', '71102', 'GMSNFD07', 'TRSKLD03', 'BA1508', '10508', 'BA0509', '70506', 'BA1311', 'BA0716', 'TRSGKA04', 'TRSTZG02', 'TRSSDA01', 'ADSJAD07', 'TRSDGA07', 'TRSWKR08', 'TRSDGA01', '11009', 'ADSYLA02', 'TRSKRM10', '10109', 'TRSTTM07', 'TRSBBB03', 'TRSBBB09', '12003', 'TRSBBB04', 'TRSBBB05', 'TRSBBB08', 'TRSKLD04', 'TRSKLD05', 'TRSKLD07', 'TRSSDA03', 'TRSKLD01', 'TRSKLD10', 'ADSMDG05', 'ADSMCH13', 'ADSGMB07', '10205', 'BA1004', '71208', 'BA0408', 'BA0416', 'TRSBBB07', 'ADSMCH15', 'ADSMCH09', 'ADSMCH11', '12414', 'BA1504', 'GMSFKY09', 'TRSTZG04', 'GMSFKY08', 'BA1506', 'BA0804', '70801', 'TRSKRM09', 'ADSMCH04', '12704', '12707', 'ADSMDG09', 'ADSMDG06', 'ADSMDG01', 'ADSMDG02', 'ADSMDG07', 'ADSMUB07', 'ADSMCH16', 'ADSMUB02', 'ADSMAH02', 'TRSDGA03', 'ADSMAH03', 'ADSMAH01', 'ADSSHG07', 'TRSDGA09', 'ADSNUM05', 'ADSDSA05', 'ADSNUM01', 'ADSDSA10', 'ADSDSA08', 'ADSDSA01', 'ADSNUM07', 'ADSSHG09', 'GMSBLG03', 'GMSBLG01', '12705', 'ADSMCH14', 'ADSTNG08', 'ADSMCH06', 'ADSMCH02', 'ADSGAN09', 'ADSMCH07', 'ADSMCH05', 'ADSGYL02', 'ADSMCH01', 'ADSMCH12', 'GMSBLG07', 'ADSMAH06', 'ADSGYL04', 'ADSMAH07', 'ADSGAN04', '11113', '10702', '12113', 'TRSTTM05', '11104', '10603', '11207', '10403', '11109', '11904', '10509', '10106', '12605', '10606', '10601', 'ADSMCH10', 'ADSGYL08', 'ADSMCH08', 'ADSMUB01', 'ADSGMB08', 'ADSGMB09', 'ADSDSA06', 'ADSYLN01', 'ADSYLN05', 'BA0206', 'ADSYLN11', 'ADSTNG09', 'ADSYLN02', 'ADSYLN09', 'TRSBBB02', 'ADSYLN08', 'ADSYLA08', 'ADSTNG10', 'ADSTNG02', 'ADSTNG03', 'ADSTNG06', 'ADSLMR03', 'ADSGUY06', '10602', 'ADSLMR09', 'ADSGAN02', 'ADSLMR06', 'TRSGKA02', 'ADSNUM02', 'TRSGKA06', 'ADSTNG04', 'ADSTNG05', 'ADSTNG01', 'TRSBAL03', 'ADSSHG08', 'TRSGKA05', 'ADSSNG07', 'ADSSNG03', 'ADSSNG06', 'ADSSNG04', 'ADSSNG01', 'ADSSNG09', 'TRSSDA11', 'ADSSNG02', 'ADSSHG02', 'ADSGMB10', 'ADSGUY08', 'ADSSHG03', 'ADSGMB02', 'ADSMWA04', 'ADSMWA12', 'ADSMWA03', 'ADSHNG02', 'ADSSNG11', 'TRSUSS11', 'ADSSNG05', 'ADSHNG07', 'GMSBLG02', 'ADSGUY03', 'ADSHNG03', 'ADSGYL06', 'ADSMUB09', 'ADSGYL09', 'ADSMUB04', 'ADSGUY02', 'ADSHNG11', 'ADSMUB05', 'ADSGUY10', 'GMSBLG08', 'ADSGUY09', 'ADSGUY05', 'ADSGUY07', 'ADSHNG06', 'ADSDSA07', 'ADSGRE01', 'ADSGMB06', 'ADSGMB04', 'ADSFUR09', 'ADSFUR04', 'ADSYLA03', 'GMSFKY07', 'ADSGMB03', 'ADSGMB01', 'ADSFUR10', 'ADSFUR02', 'ADSJAD06', 'ADSJAD11', 'TRSGKA01', 'TRSBAL08', 'ADSSHG05', '71202', 'ADSMWA11', 'ADSMWA05', 'ADSJAD04', 'ADSJAD08', '70509', '11903', '70512', '12706', '12306', 'ADSSHG04', 'ADSHNG08', 'ADSHNG01', 'ADSSHG01', '10604', '10506', '10708', 'ADSMCH03', 'ADSFUR01', 'ADSFUR05', 'ADSGRE09', 'ADSJAD05', 'ADSGAN06', '11907', 'ADSFUR11', 'ADSGAN08', 'GMSBLR04', 'GMSBLG09', 'GMSBLR07', 'ADSGUY04', 'GMSBLR06', 'ADSGUY01', 'ADSSHG10', '11308', 'BA1805', '12301', '10903', '12607', 'ADSYLN04', 'ADSSNG08', 'ADSYLA04', 'ADSYLA06', 'ADSYLA09', 'TRSLAU09', 'ADSHNG05', '70605', 'ADSDSA03', 'ADSDSA09', '10211', 'ADSHNG12', 'ADSMUB06', 'ADSDSA02', 'BA1716', 'BA1308', 'BA1706', 'BA0805', 'ADSFUR03', 'ADSFUR08', 'ADSGRE03', 'BA1715', '11504', 'BA1714', 'BA1710', 'BA1913', 'BA0815', 'BA1903', 'BA0310', 'BA1713', 'BA1702', '10208', '10507', 'TRSGAS03', '12701', '10206', '12708', '12415', 'GMSNFD01', '12013', 'TRSLAU04', '10706', '11206', 'ADSMWA09', 'ADSYLA11', 'ADSGMB05', 'ADSMWA07', '10504', '70502', '70207', 'ADSGRE06', '10409', 'ADSGRE10', 'TRSTZG05', '10610', 'TRSTZG09', '12406', '12404', '10608', '10703', '10503', 'ADSGRE07', '11311', 'ADSSHG06', 'ADSLMR07', 'ADSLMR05', 'ADSNUM04', 'ADSNUM10', 'ADSLMR08', 'ADSLMR04', 'ADSNUM09', 'ADSNUM08', 'ADSNUM06', 'ADSDSA04', '11307', '10902', '10101', '10308', '11407', '11413', '11405', 'GMSNFD05', '11910', '10510', '11508', '11204', '11212', '70408', '70809', 'GMSYDB02', '10402', '70209', '70604', '70210', '70208', '70505', '70510', '10204', '10203', 'TRSYRR01', '70812', 'TRSYRR06', '10407', 'GMSAKK08', '70401', '70405', '70501', 'TRSARD03', '70410', '10807', '10710', '12105', '10202', '70710', '70803', '70607', '71601', 'ADSLMR10', '70511', '70804', 'ADSMWA08', 'ADSMWA10', 'ADSJAD01', 'ADSNUM03', 'ADSFUR06', '70305', '70306', '70703', 'ADSGAN01', 'ADSJAD10', 'ADSGAN05', 'GMSKLT01', 'TRSBAL04', '11002', 'BA0606', 'GMSKLT06', 'GMSKLT02', 'BA0811', 'BA0814', '71503', 'BA0208', 'GMSDKU09', 'GMSDKU02', 'GMSAKK06', '35010_01', 'GMSSHM07', 'GMSFKY06', 'GMSKWM07', 'GMSAKK01', '35008_01', '12304', '12305', '12309', '12311', 'TRSJAL05', 'TRSJAL01', '12412', 'TRSJAL04', 'TRSJAL08', '16007_01', 'TRSTTM03', 'TRSUSS09', 'GMSYDB08', 'GMSYDB01', 'TRSDGA10', 'GMSYDB05', 'TRSGAS01', 'GMSYDB10', 'TRSGKA07', 'GMSAKK11', 'GMSNFD03', '16003_01', '11901', 'ADSMWA06', 'ADSGAN10', '35015_01', 'TRSUSS07', 'TRSUSS08', '11402', 'ADSTNG07', 'ADSGRE05', 'ADSFUR07', '10209', 'GMSNFD10', 'ADSJAD09', 'GMSKWM05', '70807', 'ADSSNG10', '71403', '70806', '10705', 'GMSFKY02', 'GMSFKY01', 'TRSUSS05', 'TRSTTM11', 'TRSTTM04', 'TRSUSS03', '11403', '11912', '10404', '10408', '10305', '10313', '10303', '10302', '10307', 'ADSGAN07', '11506', '10806', '10801', '71101', 'BA0306', '10805', '11106', '10810', '12507', '12501', '12509', 'BA0506', 'BA0508', 'ADSGRE02', 'BA0510', 'BA0501', 'BA0515', 'BA1002', 'TRSBBB10', 'BA1013', 'BA0412', 'BA0813', 'BA1416', 'BA0413', 'BA1219', 'BA1010', 'BA1709', 'BA1003', 'BA1008', 'BA1006', 'BA1015', 'BA1001', 'BA1011', 'BA1016', 'BA0507', 'BA0503', 'TRSGKA09', 'BA1012', 'ADSGRE08', 'BA1009', 'BA2015', 'BA1014', '10607', 'BA0218', 'BA1007', 'BA1806', 'BA0801', 'BA0513', 'GMSKWM04', 'BA0514', '71504', 'ADSYLN10', '70608', 'ADSMDG08', 'ADSMDG10', 'ADSMUB08', 'ADSMUB10', 'ADSYLN07', '71508', 'BA0217', '12702', '11203', '11802', '11202', '70606', '11302', '71205', '70508', 'BA0709', 'BA0207', 'BA0209', '10511', 'BA1616', 'BA0211', 'GMSDKU04', 'BA0219', 'BA1510', 'BA0802', 'BA0517', 'BA0216', 'BA0214', 'ADSLMR01', 'BA1707', 'BA0204', 'BA0905', 'BA0903', 'BA0908', 'BA0902', '12709', '71603', '70310', '70301', '70302', '70303', '71204', '71201', '70201', '71509', '70704', '70406', '70904', '71008', 'BA1808', '71304', '71211', '71708', '70906', '70907', '70903', '71306', '70403', '70407', '70705', '71507', '70110', 'BA1911', '70309', '70909', '70206', '71404', '71206', '12410', '12405', '12402', '11305', '10502', '11612', '11309', '12408', 'TRSWKR03', 'ADSJAD03', 'TRSKRM03', 'TRSLAU10', 'TRSWKR09', 'TRSWKR10', 'TRSWKR02', 'TRSLAU06', 'TRSLAU03', 'TRSTZG06', 'TRSTZG03', 'TRSLAU07', 'BA1802', 'TRSYRR08', 'BA1812', 'TRSYRR04', 'TRSYRR05', 'TRSYRR02', 'TRSYRR09', 'ADSYLN03', 'BA1815', 'BA1511', 'BA1618', 'BA1513', 'BA1518', 'BA1803', 'ADSMWA01', 'TRSKRM04', '35016_01', 'TRSJAL03', 'GMSKWM10', '35014_01', 'BA0904', 'ADSLMR02', 'BA1213', 'BA1813', 'BA0909', '10201', '10314', '10210', '10808', '12502', 'GMSDKU01', '12508', 'BA1505', '10809', '11702', '11706', 'BA0104', '10908', '12604', '12608', '71501', 'BA1502', 'BA1804', 'BA1811', '10102', '11501', 'GMSBLG10', '11505', '12506', '10309', '10306', 'BA0612', '10301', 'BA1703', '12505', '71610', '10804', '10803', '10802', '71607', 'TRSTZG07', '10207', '10605', '10501', '10701', '12007', '11112', '11111', '12312', '11909', '11607', 'BA1809', '11601', '11604', '11606', '12101', 'BA1817', 'BA1810', '11105', '11509', 'BA0512', 'BA0607', '11510', '11902', '10711', 'TRSJAL10', '10707', '35006_01', 'BA1514', '10312', 'BA0605', 'BA0502', 'BA0511', '70404', '12008', '12004', '12411', '12001', '70707', '70708', 'BA0601', '71006', 'BA0609', 'GMSKWM01', '11507', '12611', '10310', '10311', '10304', '71210', 'ADSMDG04', 'ADSMDG03', '70702', '70805', 'GMSGME11', '12002', 'GMSGME06', '11102', 'GMSGME02', '11010', 'GMSBLG05', '11701', '71701', 'GMSSHM08', '11810', 'BA1507', '12413', '11809', 'BA1816', '12603', 'BA0113', '12610', '12005', '12612', 'BA1503', 'TRSBAL05', '12609', 'GMSKWM06', 'GMSGME01', 'GMSGME09', '11412', '11411', '11409', 'BA1814', '71105', '11401', 'BA0302', '11410', '11406', 'TRSYRR07', '71609', 'TRSYRR10', '71605', '70409', 'ADSMAH04', 'ADSMAH05', 'BA1517', 'BA1113', 'BA1701', 'TRSLAU08', 'BA0313', 'BA0312', 'BA1501', '11110', 'BA1807', '12504', '12503', 'BA1509', 'ADSYLA05', '11908', '11208', '11906', '11911', '11304', '12011', 'BA1708', '11310', 'BA0303', '12009', '70602', '10911', '10406', '10505', '10410', '71209', '11408', '70802', '12602', '70908', '11108', '71709', '11306', 'BA1909', 'BA1801', '11211', '70905', '12401', '11609', '70513', '11610', '11103', '11603', '11201', '11205', '11303', '11312', '11415', '12110', '11414', '11107', '11611', '11608', '71401', 'GMSYDB06', '11101', 'ADSGYL07', '12206', '12209', 'BA0912', '70307', 'GMSYDB04', 'TRSJAL07', '70601', '10906', '70402', '12601', 'BA0907', '11001', '12010', '12711', '11806', '10401', '10905', '11704', '11707', 'BA1217', '12207', '12208', '70808', '11805', '10904', '11301', '12006', '10901', '11602', '12204', 'GMSYDB03', '12210', '70308', '71506', 'TRSYRR03', 'BA0608', 'BA0803', 'GMSDKU10', 'BA1305', 'BA1705', 'BA1901', 'BA0610', 'GMSBLR09', 'BA0304', 'GMSBLR01', 'BA1704', 'BA0603', 'BA0309', 'BA0308', 'BA0305', 'BA1904', 'BA0311', 'BA1512', 'BA1005', 'BA1906', 'BA1101', '71103', 'BA1212', 'BA1712', 'BA1105', '70610', 'BA0415', '71107', 'BA1515', 'TRSGAS05', '11804', 'BA1516', 'GMSDKU08', 'BA1711', '70507', 'BA0220', 'BA1908', 'BA1108', '70611', 'TRSBAL02', '35003_01', '70609', 'TRSTZG01', 'BA1110', 'BA0702', 'BA0201', 'GMSFKY10', 'BA1111', 'BA1106', 'BA1902', 'ADSMWA02', '71106', 'BA0203', 'BA0213', 'BA1910', 'BA0714', 'BA1613', 'BA0816', '71305', '70202', 'TRSKLD11', 'BA1112', '71104', 'BA0202', 'GMSSHM09', 'TRSWKR01', 'BA1604', 'BA1602', 'BA0405', 'BA1614', 'GMSYDB09', 'ADSGYL05', 'BA0703', 'BA0205', 'TRSBAL06', 'BA1907', 'TRSTTM08', 'BA0808', 'BA0215', 'BA1104', '11807', 'TRSLAU05', 'BA1601', 'BA0807', '12102', '11503', 'TRSTTM06', 'ADSGRE04', 'BA1103', 'BA0106', '71108', 'BA1107', 'BA0809', 'TRSKRM05', 'BA1606', 'TRSGAS10', 'BA0107', 'BA0810', 'BA1609', '71301', 'BA1109', 'BA1401', 'TRSKRM06', 'BA0212', '12112', 'TRSSDA02', 'BA1309', 'BA0108', 'BA1102', 'TRSGAS07', '70910', 'BA0410', 'BA1611', 'BA1612', 'BA0210', '12403', 'BA1203', '70101', 'TRSBAL07', '71110', 'BA1608', 'BA1214', 'TRSTTM02', 'GMSDKU05', '70706', '11210', 'BA0806', '11209', 'BA1216', '71505', 'GMSDKU07', '70304', '71502', 'ADSMUB11', 'ADSHNG09', '11710', 'BA0812', 'BA1302', 'BA0119', 'BA1617', '11005', 'BA1207', '71307', '12307', 'BA0103', '12302', '12310', '12303', '11705', '11708', 'BA2004', '12308', 'BA2012', '11709', 'BA2017', '11008', 'BA1206', 'GMSBLG04', 'BA2010', '11905', '10405', 'ADSMUB03', '12703', 'BA2007', 'BA2008', 'TRSGKA03', 'BA0114', '71703', 'BA0117', 'BA1218', 'BA0906', 'BA1202', 'BA1201', 'BA0118', 'BA0115', 'TRSGAS06', 'TRSBAL11', 'BA0901', '11003', 'BA1306', 'TRSGKA08', '70701', 'BA2005', '10907', '12606', 'BA2003', '70709', 'BA0913', 'BA1414', '11007', '71407', '11006', 'BA2006', 'BA2013', '10910', 'BA1301', '11004', 'BA1205', '71602', 'BA2018', '71608', 'BA0910', '71604', '71606', 'BA1402', 'BA0112', '12205', 'BA2011', 'BA1204', '12203', 'BA0715', '12201', 'BA1405', 'BA1411', 'BA0404', 'GMSKWM03', 'TRSBAL09', 'BA1215', 'TRSBAL10', '10104', '71310', 'BA1208', 'BA0504', 'BA2009', 'BA1406', 'GMSNFD09', 'BA0101', '71710', 'TRSKRM07', 'BA0704', 'TRSKLD06', 'BA1304', 'BA1312', 'BA1303', 'BA2014', 'BA1415', 'BA1404', 'BA0613', 'BA1410', 'TRSKLD09', '71003', 'TRSSDA10', 'GMSSHM01', 'BA0604', 'BA1310', 'BA1211', '71308', 'BA1407', 'BA1307', 'BA1412', 'BA0611', 'BA1403', 'BA0414', 'BA0102', 'BA1210', 'BA0717', 'BA1209', '12012', 'BA0710', 'ADSYLA01', 'BA0505', 'BA0516', '71009', 'BA0712', 'BA1408', 'BA0402', '71001', 'GMSAKK07', 'BA0701', 'BA0116', 'BA1413', 'BA0403', '71302', 'GMSDKU06', '71309', 'TRSGAS02', 'BA0705', '71706', 'TRSGAS12', 'BA0706', 'BA0711', 'TRSGAS04', 'BA0407', 'BA2016', '70901', '70106', 'BA0110', 'GMSAKK05', 'BA0109', '71007', 'BA0707', '71705', 'BA0411', '11404', '70204', '35001_01', 'TRSJAL02', 'BA2001', 'TRSGAS09', 'BA0105', '70107', 'BA0401', '71406', '70902', 'GMSAKK03', '71004', '70503', '71002', 'BA1313', '10212', 'TRSGAS08', 'BA0718', '10213', 'TRSBAL01', '70603', 'GMSAKK09', '70104', 'BA0409', '12409', 'BA0406', 'GMSAKK04', '71409', 'TRSGKA10', '71203', '71207', 'GMSAKK10', '71005', 'TRSKRM02', '70102', '70103', '71405', '71702', '70504', '70105', '70108', '11801', '10609', '70203', '70109', '11808', '71408', '71402', '70205', 'TRSJAL09', 'TRSJAL06', 'ADSMAH10', '12710', '10611', 'ADSYLA10', 'ADSJAD02', '12202', '10110', '10107', '12111', '10111', '12106', 'ADSGYL10', '11703', '11502', 'GMSBLR08', 'TRSLAU01', 'TRSTZG08', 'TRSLAU02', 'TRSTZG10', 'TRSWKR05', 'TRSWKR07', 'TRSUSS02', 'TRSTTM01', '71410', 'GMSNFD06', '71510'][]);


create table northeast_pop_wards as 
	SELECT grid_data.boundary_vaccwards.globalid,grid_data.boundary_vaccwards.wardcode,
	grid_data.boundary_vaccwards.wardname,grid_data.boundary_vaccwards.lgacode,sum(one_four) one_four, sum(five_nine) five_nine, sum(ten_fourteen) ten_fourteen, 
       sum(fifteen_nineteen) fifteen_nineteen, sum(twenty_twentyfour) twenty_twentyfour, sum(twentyfive_twentynine) twentyfive_twentynine
       , sum(thirty_thirtyfour) thirty_thirtyfour,sum(thirtyfive_thirtynine) thirtyfive_thirtynine, sum(forty_fortyfour) forty_fortyfour
       , sum(fortyfive_fortynine) fortyfive_fortynine,sum(fifty_fiftyfour)fifty_fiftyfour, sum(fiftyfive_fiftynine) fiftyfive_fiftynine, 
       sum(sixty_sixtyfour) sixty_sixtyfour, sum(sixtyfive_sixtynine)sixtyfive_sixtynine, 
       sum (seventy_seventyfour) seventy_seventyfour, sum(seventyfive_hundred) seventyfive_hundred, sum(total_pp) total_pp
       ,geom
  FROM public.northeast_pp2_vts join grid_data.boundary_vaccwards on public.northeast_pp2_vts.wardcode =  grid_data.boundary_vaccwards.wardcode
  group by grid_data.boundary_vaccwards.wardcode,grid_data.boundary_vaccwards.globalid,
grid_data.boundary_vaccwards.wardname,grid_data.boundary_vaccwards.lgacode,grid_data.boundary_vaccwards.geom;

create view lga_pp
SELECT grid_data.boundary_vacclgas.globalid,grid_data.boundary_vacclgas.lgacode, lganame,statecode,sum(one_four) one_four, sum(five_nine) five_nine, sum(ten_fourteen) ten_fourteen, 
       sum(fifteen_nineteen) fifteen_nineteen, sum(twenty_twentyfour) twenty_twentyfour, sum(twentyfive_twentynine) twentyfive_twentynine
       , sum(thirty_thirtyfour) thirty_thirtyfour,sum(thirtyfive_thirtynine) thirtyfive_thirtynine, sum(forty_fortyfour) forty_fortyfour
       , sum(fortyfive_fortynine) fortyfive_fortynine,sum(fifty_fiftyfour)fifty_fiftyfour, sum(fiftyfive_fiftynine) fiftyfive_fiftynine, 
       sum(sixty_sixtyfour) sixty_sixtyfour, sum(sixtyfive_sixtynine)sixtyfive_sixtynine, 
       sum (seventy_seventyfour) seventy_seventyfour, sum(seventyfive_hundred) seventyfive_hundred, sum(total_pp) total_pp
       , grid_data.boundary_vacclgas.geom
  FROM northeast_ward_pp2 join grid_data.boundary_vacclgas on northeast_ward_pp2.lgacode =  grid_data.boundary_vacclgas.lgacode
  group by grid_data.boundary_vacclgas.lgacode,grid_data.boundary_vacclgas.globalid, lganame,statecode,grid_data.boundary_vacclgas.geom;

create view northcentral_state_pp
SELECT grid_data.boundary_vaccstates.globalid,statename,grid_data.boundary_vaccstates.statecode,sum(one_four) one_four, sum(five_nine) five_nine, sum(ten_fourteen) ten_fourteen, 
       sum(fifteen_nineteen) fifteen_nineteen, sum(twenty_twentyfour) twenty_twentyfour, sum(twentyfive_twentynine) twentyfive_twentynine
       , sum(thirty_thirtyfour) thirty_thirtyfour,sum(thirtyfive_thirtynine) thirtyfive_thirtynine, sum(forty_fortyfour) forty_fortyfour
       , sum(fortyfive_fortynine) fortyfive_fortynine,sum(fifty_fiftyfour)fifty_fiftyfour, sum(fiftyfive_fiftynine) fiftyfive_fiftynine, 
       sum(sixty_sixtyfour) sixty_sixtyfour, sum(sixtyfive_sixtynine)sixtyfive_sixtynine, 
       sum (seventy_seventyfour) seventy_seventyfour, sum(seventyfive_hundred) seventyfive_hundred, sum(total_pp) total_pp
       , grid_data.boundary_vaccstates.geom
   FROM public.northcentral_lga_pp2 join grid_data.boundary_vaccstates on public.northcentral_lga_pp2.statecode =  grid_data.boundary_vaccstates.statecode
  group by grid_data.boundary_vaccstates.globalid, statename,grid_data.boundary_vaccstates.statecode,grid_data.boundary_vaccstates.geom;
 
  