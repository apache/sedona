/**
 * Copyright 2019 - 2021 Jia Yu (jiayu@apache.org)
 * Copyright 2017 Volume Integration
 * Copyright 2017 Tom Grant
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
