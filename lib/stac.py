import dataclasses
import pathlib
import rasterio

from .dem import SetsmDem, SetsmTile

COLLECTIONS = {
    "arcticdem-mosaics-v3.0-2m",
    "arcticdem-mosaics-v3.0-10m",
    "arcticdem-mosaics-v3.0-32m",
    "arcticdem-mosaics-v4.1-2m",
    "arcticdem-mosaics-v4.1-10m",
    "arcticdem-mosaics-v4.1-32m",
    "arcticdem-strips-s2s041-2m",
    "earthdem-strips-s2s041-2m",
    "rema-mosaics-v2.0-2m",
    "rema-mosaics-v2.0-10m",
    "rema-mosaics-v2.0-32m",
    "rema-strips-s2s041-2m",
}


@dataclasses.dataclass(frozen=True)
class RasterAssetInfo:
    nodata: int | float
    data_type: str
    gsd: float
    proj_code: str
    proj_shape: list[int]
    proj_transform: list[float]
    proj_bbox: list[float]
    proj_geojson: dict
    proj_centroid: list[float]

    @classmethod
    def from_raster(cls, filepath: str | pathlib.Path):
        with rasterio.open(filepath, "r") as src:
            if not src.crs.is_projected:
                raise ValueError(f"{filepath} does not use a projected CRS")

            authority, code = src.crs.to_authority()
            x_min, y_min, x_max, y_max = src.bounds
            proj_geojson = {
                "type": "Polygon",
                "coordinates": [
                    [
                        (x_min, y_min),  # lower left
                        (x_max, y_min),  # lower right
                        (x_max, y_max),  # upper right
                        (x_min, y_max),  # upper left
                        (x_min, y_min),  # lower left again
                    ]
                ],
            }

            centroid_long, centroid_lat = src.lnglat()

            return cls(
                nodata=src.nodata,
                data_type=src.dtypes[0],
                gsd=(src.res[0] + src.res[1]) / 2.0,
                proj_code=f"{authority}:{code}",
                proj_shape=[src.height, src.width],
                proj_transform=list(src.transform),
                proj_bbox=list(src.bounds),
                proj_geojson=proj_geojson,
                # The center of the raster in [lat, long] per the projection extension
                # https://github.com/stac-extensions/projection?tab=readme-ov-file#projcentroid
                proj_centroid=[centroid_lat, centroid_long],
            )


class StacHrefBuilder:
    def __init__(
            self, base_url: str, s3_bucket: str, domain: str,
            raster: SetsmDem | SetsmTile
    ):
        self.base_url = base_url.rstrip("/")  # Remove trailing slash if present
        self.base_s3_url = f"s3://{s3_bucket}"

        if isinstance(raster, SetsmDem):
            kind = "strips"
            geocell_or_supertile = raster.geocell
            release_version = raster.release_version
            res_str = raster.res_str
            item_id = raster.stripid
        elif isinstance(raster, SetsmTile):
            kind = "mosaics"
            geocell_or_supertile = raster.supertile_id_no_res
            release_version = f"v{raster.release_version}"
            res_str = raster.res
            item_id = raster.tileid
        else:
            raise ValueError(
                f"raster argument must be either {type(SetsmDem)} or {type(SetsmTile)}. Got {type(raster)}"
            )

        self._partial_asset_key = f"{domain}/{kind}/{release_version}/{res_str}/{geocell_or_supertile}"
        self._item_key = f"{domain}/{kind}/{release_version}/{res_str}/{geocell_or_supertile}/{item_id}.json"
        self._catalog_key = f"{domain}/{kind}/{release_version}/{res_str}/{geocell_or_supertile}.json"
        self._collection_key = f"{domain}/{kind}/{release_version}/{res_str}.json"
        self._root_key = "pgc-data-stac.json"

    def item_href(self, as_s3: bool = False) -> str:
        base = self.base_s3_url if as_s3 else self.base_url
        return f"{base}/{self._item_key}"

    def catalog_href(self, as_s3: bool = False) -> str:
        base = self.base_s3_url if as_s3 else self.base_url
        return f"{base}/{self._catalog_key}"

    def collection_href(self, as_s3: bool = False) -> str:
        base = self.base_s3_url if as_s3 else self.base_url
        return f"{base}/{self._collection_key}"

    def root_href(self, as_s3: bool = False) -> str:
        base = self.base_s3_url if as_s3 else self.base_url
        return f"{base}/{self._root_key}"

    def asset_href(self, filepath: str | pathlib.Path, as_s3: bool = False) -> str:
        filename = pathlib.Path(filepath).name if isinstance(filepath,
                                                             str) else filepath.name
        base = self.base_s3_url if as_s3 else self.base_url
        return f"{base}/{self._partial_asset_key}/{filename}"
