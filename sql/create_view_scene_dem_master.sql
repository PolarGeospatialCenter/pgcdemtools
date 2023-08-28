create materialized view dem.scene_dem_master as
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
       (d.stripdemid IS NOT NULL) AS is_depr,
       a.wkb_geometry
FROM dem.scene_dem_all a
         JOIN (SELECT "left"(b.stripdemid, '52') AS strip_nover,
                      max(b.stripdemid)                   AS strip_max,
                      b.is_dsp
               FROM (SELECT DISTINCT scene_dem_all.stripdemid,
                                     scene_dem_all.is_dsp
                     FROM dem.scene_dem_all) b
               GROUP BY strip_nover, b.is_dsp) c
              ON a.stripdemid = c.strip_max AND a.is_dsp = c.is_dsp
         LEFT JOIN (SELECT DISTINCT stripdemid_deprecated.stripdemid
                    FROM dem.stripdemid_deprecated) d ON a.stripdemid = d.stripdemid
WHERE a.is_dsp = false;

comment on materialized view dem.scene_dem_master is 'Last Updated 2023-03-07';

alter materialized view dem.scene_dem_master owner to pgc_gis_admin;

create unique index scenedem_stripdem_master_idx
    on dem.scene_dem_master (scenedemid, stripdemid);

grant select on dem.scene_dem_master to pgc_users;

grant select on dem.scene_dem_master to backup;

