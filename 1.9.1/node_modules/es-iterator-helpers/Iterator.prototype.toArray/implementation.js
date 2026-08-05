'use strict';

var $TypeError = require('es-errors/type');

var GetIteratorDirect = require('es-abstract/2025/GetIteratorDirect');
var IteratorStepValue = require('es-abstract/2025/IteratorStepValue');

var isObject = require('es-abstract/helpers/isObject');

module.exports = function toArray() {
	if (this instanceof toArray) {
		throw new $TypeError('`toArray` is not a constructor');
	}

	var O = this; // step 1

	if (!isObject(O)) {
		throw new $TypeError('`this` value must be an Object'); // step 2
	}

	var iterated = GetIteratorDirect(O); // step 3

	var items = []; // step 4

	while (true) { // step 5
		var value = IteratorStepValue(iterated); // step 5.a
		if (iterated['[[Done]]']) {
			return items; // step 5.b
		}
		items[items.length] = value; // step 5.d
	}
};
