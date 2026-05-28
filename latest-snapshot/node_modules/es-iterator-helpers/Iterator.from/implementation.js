'use strict';

var $TypeError = require('es-errors/type');

var GetIteratorFlattenable = require('es-abstract/2025/GetIteratorFlattenable');
var OrdinaryHasInstance = require('es-abstract/2025/OrdinaryHasInstance');
var OrdinaryObjectCreate = require('es-abstract/2025/OrdinaryObjectCreate');

var $Iterator = require('../Iterator/polyfill')();
var $WrapForValidIteratorPrototype = require('../WrapForValidIteratorPrototype');

var SLOT = require('internal-slot');

module.exports = function from(O) {
	if (this instanceof from) {
		throw new $TypeError('`Iterator.from` is not a constructor');
	}

	var iteratorRecord = GetIteratorFlattenable(O, 'ITERATE-STRING-PRIMITIVES'); // step 1

	var hasInstance = OrdinaryHasInstance($Iterator, iteratorRecord['[[Iterator]]']); // step 2

	if (hasInstance) { // step 3
		return iteratorRecord['[[Iterator]]']; // step 3.a
	}

	var wrapper = OrdinaryObjectCreate($WrapForValidIteratorPrototype); // , ['[[Iterated]]']); // step 4

	SLOT.set(wrapper, '[[Iterated]]', iteratorRecord); // step 5

	return wrapper; // step 6
};
