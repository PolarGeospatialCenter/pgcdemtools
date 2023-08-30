create materialized view dem.strip_dem_master as
WITH latest_version AS (
    SELECT left(b.stripdemid, '-8') AS strip_nover,
           max(b.stripdemid)        AS strip_max
    FROM dem.strip_dem_all b
    GROUP BY ("left"(b.stripdemid, '-8'))
),
     latest_lsf AS (
         SELECT strip_dem_all_1.stripdemid,
                bool_and(strip_dem_all_1.is_lsf) AS min_lsf
         FROM dem.strip_dem_all strip_dem_all_1
                  JOIN latest_version ON strip_dem_all_1.stripdemid = latest_version.strip_max
         GROUP BY strip_dem_all_1.stripdemid
     )
SELECT strip_dem_all.ogc_fid,
       strip_dem_all.dem_id,
       strip_dem_all.stripdemid,
       strip_dem_all.pairname,
       strip_dem_all.sensor1,
       strip_dem_all.sensor2,
       strip_dem_all.acqdate1,
       strip_dem_all.acqdate2,
       strip_dem_all.avgacqtm1,
       strip_dem_all.avgacqtm2,
       strip_dem_all.catalogid1,
       strip_dem_all.catalogid2,
       strip_dem_all.cent_lat,
       strip_dem_all.cent_lon,
       strip_dem_all.geocell,
       strip_dem_all.region,
       strip_dem_all.epsg,
       strip_dem_all.proj4,
       strip_dem_all.nd_value,
       strip_dem_all.dem_res,
       strip_dem_all.cr_date,
       strip_dem_all.algm_ver,
       strip_dem_all.s2s_ver,
       strip_dem_all.is_lsf,
       strip_dem_all.is_xtrack,
       strip_dem_all.edgemask,
       strip_dem_all.watermask,
       strip_dem_all.cloudmask,
       strip_dem_all.mask_dens,
       strip_dem_all.valid_dens,
       strip_dem_all.valid_area,
       strip_dem_all.valid_perc,
       strip_dem_all.water_area,
       strip_dem_all.water_perc,
       strip_dem_all.cloud_area,
       strip_dem_all.cloud_perc,
       strip_dem_all.avgconvang,
       strip_dem_all.avg_ht_acc,
       strip_dem_all.avg_sunel1,
       strip_dem_all.avg_sunel2,
       strip_dem_all.rmse,
       strip_dem_all.location,
       strip_dem_all.filesz_dem,
       strip_dem_all.filesz_mt,
       strip_dem_all.filesz_or,
       strip_dem_all.filesz_or2,
       strip_dem_all.index_date,
       strip_dem_all.wkb_geometry
FROM dem.strip_dem_all
         JOIN latest_lsf ON strip_dem_all.stripdemid = latest_lsf.stripdemid AND
                            strip_dem_all.is_lsf = latest_lsf.min_lsf;

comment on materialized view dem.strip_dem_master is 'Strip DEMs from strip_dem that belong to canonical stripdemids. Canonical is defined as the latest SETSM version of a strip imagery pair and resolution.';

alter materialized view dem.strip_dem_master owner to pgc_gis_admin;

grant select on dem.strip_dem_master to pgc_users;
