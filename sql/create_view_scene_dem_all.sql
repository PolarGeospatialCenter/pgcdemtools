create materialized view scene_dem_all as
WITH dems_on_hand AS (
    SELECT scene_dem.scenedemid,
           scene_dem.stripdemid
    FROM dem.scene_dem
    WHERE scene_dem.dem_res = 0.5
    UNION ALL
    SELECT scene_dem_staging.scenedemid,
           scene_dem_staging.stripdemid
    FROM dem.scene_dem_staging
    WHERE scene_dem_staging.dem_res = 0.5
)
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
                 sds.is_dsp = tbl2.is_dsp AND (sds.location || sds.index_date) = tbl2.loc_idate
UNION ALL
SELECT b.scenedemid,
       b.stripdemid,
       'aws'::character varying AS status,
       c.pairname,
       c.sensor1,
       c.sensor2,
       c.acqdate1,
       c.acqdate2,
       c.catalogid1,
       c.catalogid2,
       c.cent_lat,
       c.cent_lon,
       c.region,
       c.epsg,
       c.proj4,
       c.nd_value,
       c.dem_res,
       c.cr_date,
       c.algm_ver,
       c.prod_ver,
       c.has_lsf,
       c.has_nonlsf,
       c.is_dsp,
       c.is_xtrack,
       c.scene1,
       c.scene2,
       c.gen_time1,
       c.gen_time2,
       NULL::character varying  AS location,
       b.filesz_dem,
       c.filesz_lsf,
       b.filesz_mt,
       b.filesz_or,
       b.filesz_or2,
       NULL::timestamptz  AS index_date,
       c.wkb_geometry
FROM (SELECT min(csda.path::text) AS path
      FROM dem.csdap_pgc_dem_delivery csda
               LEFT JOIN dems_on_hand doh USING (scenedemid, stripdemid)
      WHERE doh.scenedemid IS NULL
      GROUP BY csda.scenedemid, csda.stripdemid) csda_uniq
         JOIN dem.csdap_pgc_dem_delivery b USING (path)
         LEFT JOIN dem.scene_dem_pseudo_50cm c USING (scenedemid, stripdemid);

comment on materialized view scene_dem_all is 'Concatenation of scene DEM source tables. Scene DEMs from staging that are also on tape are excluded. Scene DEMs that are on hand are removed from the CSDAP DEM table. Duplicate scenedemid-stripdemid-isdsp combos in staging are removed and only records with the smallest location and index date are kept.';

alter materialized view scene_dem_all owner to pgc_gis_admin;

create unique index scenedem_stripdem_isdsp_all_idx
    on scene_dem_all (scenedemid, stripdemid, is_dsp);

grant select on scene_dem_all to pgc_users;

grant select on scene_dem_all to backup;

