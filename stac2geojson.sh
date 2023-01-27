#!/bin/bash

JQ=$(cat <<'EOF'
{
  type: "FeatureCollection",
  features: [
    inputs |
    (.links[]|select(.rel == "self").href) as $myUrl |
    ($myUrl | split("/")[0:-1] | join("/") ) as $baseUrl |
    {
      type: .type,
      properties: {
        stripdemid: .properties["pgc:stripdemid"],
        epsg: .properties["proj:epsg"],
        start_datetime: .properties["start_datetime"],
        end_datetime: .properties["end_datetime"],
        stac_item: $myUrl,
        hillshade: ($baseUrl + .assets.hillshade.href[1:]),
        hillshade_masked:($baseUrl + .assets.hillshade_masked.href[1:]),
        dem: ($baseUrl + .assets.dem.href[1:]),
        mask: ($baseUrl + .assets.mask.href[1:]),
        matchtag: ($baseUrl + .assets.matchtag.href[1:]),
        metadata: ($baseUrl + .assets.metadata.href[1:]),
        readme: ($baseUrl + .assets.readme.href[1:])
      },
      geometry: .geometry
    }
  ]
}
EOF
)

for res in aws_opendata/*/strips/s2s041/* ; do
	if [[ ! -d "$res" ]] ; then
		continue
	fi
	for tile in "$res"/* ; do
		if [[ ! -d "$tile" ]] ; then
			continue
		fi
		echo "processing $tile"
		jq -c "$JQ" \
		   "$tile"/*.json > "$tile".geojson
	done
done
