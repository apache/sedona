/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import Visualization from 'zeppelin-vis'
import ColumnselectorTransformation from 'zeppelin-tabledata/columnselector'


import L from 'leaflet/dist/leaflet'
import 'leaflet/dist/leaflet.css'
// workaround https://github.com/Leaflet/Leaflet/issues/4968
import icon from 'leaflet/dist/images/marker-icon.png';
import iconShadow from 'leaflet/dist/images/marker-shadow.png';

let DefaultIcon = L.icon({
	iconUrl: icon,
	iconSize: [25, 41],
	iconAnchor: [12, 41],
	popupAnchor: [0, -41],
	tooltipAnchor: [12, -28],
	shadowUrl: iconShadow
});
L.Marker.prototype.options.icon = DefaultIcon;

export default class LeafletMap extends Visualization {

	constructor(targetEl, config) {
		super(targetEl, config);

		const columnSpec = [
			{name: 'mapimage'},
			{name: 'geometry'},
			{name: 'info'}
		];

		this.transformation = new ColumnselectorTransformation(config, columnSpec);
		this.chartInstance = L.map(this.getChartElementId());
	}

	getTransformation() {
		return this.transformation;
	}

	showChart() {
		super.setConfig(config);
		this.transformation.setConfig(config);
		if (!this.chartInstance) {
			this.chartInstance = L.map(this.getChartElementId());
		}
		return this.chartInstance;
	};

	getChartElementId() {
		return this.targetEl[0].id
	};

	getChartElement() {
		return document.getElementById(this.getChartElementId());
	};

	clearChart() {
		if (this.chartInstance) {
			this.chartInstance.off();
			this.chartInstance.remove();
			this.chartInstance = null;
		}
	};

	showError(error) {
		this.clearChart();
		this.getChartElement().innerHTML = `
        <div style="margin-top: 60px; text-align: center; font-weight: 100">
            <span style="font-size:30px;">
                ${error.message}
            </span>
        </div>`
	}

	drawMapChart(chartDataModel) {
		const map = this.showChart();
		L.tileLayer('http://{s}.tile.osm.org/{z}/{x}/{y}.png', {
			attribution: '&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
		}).addTo(map);

		this.getChartElement().style.height = this.targetEl.height();
		map.invalidateSize(true)

		var imageBounds = null
		const markers = chartDataModel.rows.map(
				row => {
					const {image, boundary, info} = row;
					// throw new Error(image);
					var jsts = require("jsts");
					// Read WKT string from Sedona
					var reader = new jsts.io.WKTReader();
					var obj = reader.read(boundary)
					// Collect the centroid point of the input geometry
					var centroid = obj.getCentroid()
					var marker = L.marker([centroid.getY(), centroid.getX()]);
					const mapMarker = marker.addTo(map);
					// Attach the marker information if exists
					if(info){
						mapMarker.bindTooltip(info)
					}
					// Overlay the generated image over the tile layer
					if (image) {
						var envelope = obj.getEnvelopeInternal()
						// Read the boundary of the map viewport
						imageBounds = [[envelope.getMinY(), envelope.getMinX()], [envelope.getMaxY(), envelope.getMaxX()]]
						var imageUrl = 'data:image/png;base64,' + image;
						L.imageOverlay(imageUrl, imageBounds).addTo(map);
					}
					return marker
			}
		);
		// Adjust the location of the viewport
		// If imageBounds was initialized, use imageBounds instead
		var bounds = null
		if (imageBounds) {
			bounds = imageBounds;
		}
		else {
			let featureGroup = L.featureGroup(markers);
			bounds = featureGroup.getBounds().pad(0.5);
		}
		// throw new Error(featureGroup.getBounds().pad(0.5).toString())
		map.fitBounds(bounds);

	};

	createMapDataModel(data) {
			const getColumnIndex = (config, fieldName, isOptional) => {
				const fieldConf = config[fieldName];
				if (fieldConf instanceof Object) {
					return fieldConf.index
				}
				else if (isOptional) {
					return -1
				}
				else {
					throw {
						message: "Please set " + fieldName + " in Settings"
					}
				}
			};

			const config = this.getTransformation().config;
			const imageIdx = getColumnIndex(config, 'mapimage', true);
			const boundaryIdx = getColumnIndex(config, 'geometry');
			const infoIdx = getColumnIndex(config, 'info', true);
			const rows = data.rows.map(tableRow => {
			const image = imageIdx < 0 ? null : tableRow[imageIdx];
			const boundary = tableRow[boundaryIdx];
			const info = infoIdx < 0 ? null : tableRow[infoIdx];
			return {
				image, boundary, info
			};
		});

		return {
			rows
		};
	}

	render(data) {
		try {
			const mapDataModel = this.createMapDataModel(data);
			this.clearChart();
			this.drawMapChart(mapDataModel)
		}
		catch (error) {
			console.error(error);
			this.showError(error)
		}
	}
}
