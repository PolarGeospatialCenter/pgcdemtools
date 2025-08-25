create materialized view dem.scene_dem_all as
SELECT a.scenedemid,
       a.stripdemid,
       a.status,
       a.pairname,
       a.sensor1,
       a.sensor2,
       a.acqdate1,
       a.acqdate2,
       a.catalogid1,
       a.catalogid2,
       a.cent_lat,
       a.cent_lon,
       a.region,
       a.epsg,
       a.proj4,
       a.nd_value,
       a.dem_res,
       a.cr_date,
       a.algm_ver,
       a.prod_ver,
       a.has_lsf,
       a.has_nonlsf,
       a.is_dsp,
       a.is_xtrack,
       a.scene1,
       a.scene2,
       a.gen_time1,
       a.gen_time2,
       a.location,
       a.filesz_dem,
       a.filesz_lsf,
       a.filesz_mt,
       a.filesz_or,
       a.filesz_or2,
       a.index_date,
       a.wkb_geometry
FROM dem.scene_dem a
UNION ALL
SELECT sds.scenedemid,
       sds.stripdemid,
       sds.status,
       sds.pairname,
       sds.sensor1,
       sds.sensor2,
       sds.acqdate1,
       sds.acqdate2,
       sds.catalogid1,
       sds.catalogid2,
       sds.cent_lat,
       sds.cent_lon,
       sds.region,
       sds.epsg,
       sds.proj4,
       sds.nd_value,
       sds.dem_res,
       sds.cr_date,
       sds.algm_ver,
       sds.prod_ver,
       sds.has_lsf,
       sds.has_nonlsf,
       sds.is_dsp,
       sds.is_xtrack,
       sds.scene1,
       sds.scene2,
       sds.gen_time1,
       sds.gen_time2,
       sds.location,
       sds.filesz_dem,
       sds.filesz_lsf,
       sds.filesz_mt,
       sds.filesz_or,
       sds.filesz_or2,
       sds.index_date,
       sds.wkb_geometry
FROM dem.scene_dem_staging sds
         JOIN (SELECT sds2.scenedemid,
                      sds2.stripdemid,
                      sds2.is_dsp,
                      min(sds2.location || sds2.index_date) AS loc_idate
               FROM dem.scene_dem_staging sds2
                        LEFT JOIN (SELECT scene_dem.scenedemid,
                                          scene_dem.stripdemid,
                                          scene_dem.is_dsp
                                   FROM dem.scene_dem) tbl ON tbl.scenedemid = sds2.scenedemid AND
                                                              tbl.stripdemid = sds2.stripdemid AND
                                                              tbl.is_dsp = sds2.is_dsp
               WHERE tbl.scenedemid IS NULL
               GROUP BY sds2.scenedemid, sds2.stripdemid, sds2.is_dsp) tbl2
              ON sds.scenedemid = tbl2.scenedemid AND sds.stripdemid = tbl2.stripdemid AND
                 sds.is_dsp = tbl2.is_dsp AND (sds.location || sds.index_date) = tbl2.loc_idate;

comment on materialized view dem.scene_dem_all is 'Concatenation of scene DEM source tables. Scene DEMs from staging that are also on tape are excluded. Duplicate scenedemid-stripdemid-isdsp combos in staging are removed and only records with the smallest location and index date are kept.';

alter materialized view dem.scene_dem_all owner to pgc_gis_admin;

create unique index scenedem_stripdem_isdsp_all_idx
    on dem.scene_dem_all (scenedemid, stripdemid, is_dsp);

grant select on dem.scene_dem_all to pgc_users;

grant select on dem.scene_dem_all to backup;

