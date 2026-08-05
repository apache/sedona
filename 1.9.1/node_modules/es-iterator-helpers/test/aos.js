'use strict';

var test = require('tape');
var forEach = require('for-each');
var debug = require('object-inspect');
var v = require('es-value-fixtures');

var CompletionRecord = require('es-abstract/2025/CompletionRecord');
var NormalCompletion = require('es-abstract/2025/NormalCompletion');
var ReturnCompletion = require('es-abstract/2025/ReturnCompletion');
var ThrowCompletion = require('es-abstract/2025/ThrowCompletion');

var SLOT = require('internal-slot');
var IteratorCloseAll = require('../aos/IteratorCloseAll');
var IfAbruptCloseIterators = require('../aos/IfAbruptCloseIterators');
var GetOptionsObject = require('../aos/GetOptionsObject');
var GeneratorResumeAbrupt = require('../aos/GeneratorResumeAbrupt');

var makeGenerator = function (state) {
	var gen = {};
	SLOT.set(gen, '[[GeneratorState]]', state);
	SLOT.set(gen, '[[GeneratorBrand]]', 'test');
	SLOT.set(gen, '[[GeneratorContext]]', null);
	SLOT.set(gen, '[[CloseIfAbrupt]]', null);
	return gen;
};

test('GeneratorResumeAbrupt', function (t) {
	t.test('ThrowCompletion on SUSPENDED-START state', function (st) {
		var gen = makeGenerator('SUSPENDED-START');

		st['throws'](
			function () { GeneratorResumeAbrupt(gen, ThrowCompletion(new EvalError('test')), 'test'); },
			EvalError,
			'throw completion re-throws on SUSPENDED-START (transitions to COMPLETED first)'
		);
		st.equal(SLOT.get(gen, '[[GeneratorState]]'), 'COMPLETED', 'state is COMPLETED after throw');
		st.end();
	});

	t.test('ThrowCompletion on COMPLETED state', function (st) {
		var gen = makeGenerator('COMPLETED');

		st['throws'](
			function () { GeneratorResumeAbrupt(gen, ThrowCompletion(new EvalError('test')), 'test'); },
			EvalError,
			'throw completion re-throws on COMPLETED'
		);
		st.end();
	});

	t.test('ReturnCompletion on COMPLETED state', function (st) {
		var gen = makeGenerator('COMPLETED');

		var result = GeneratorResumeAbrupt(gen, ReturnCompletion(42), 'test');
		st.deepEqual(result, { value: 42, done: true }, 'return completion returns {value, done: true}');
		st.end();
	});

	t.test('ThrowCompletion on SUSPENDED-YIELD calls genContext', function (st) {
		var gen = makeGenerator('SUSPENDED-YIELD');
		var contextCalledWith = null;
		SLOT.set(gen, '[[GeneratorContext]]', function (val) {
			contextCalledWith = val;
			return { value: undefined, done: true };
		});

		var result = GeneratorResumeAbrupt(gen, ThrowCompletion(new EvalError('injected')), 'test');
		st.ok(contextCalledWith instanceof EvalError, 'genContext called with the error value');
		st.deepEqual(result, { value: undefined, done: true }, 'returns genContext result');
		st.end();
	});

	t.test('ReturnCompletion on SUSPENDED-YIELD calls closeIfAbrupt', function (st) {
		var gen = makeGenerator('SUSPENDED-YIELD');
		var closeCalledWith = null;
		SLOT.set(gen, '[[GeneratorContext]]', function () { return { value: undefined, done: true }; });
		SLOT.set(gen, '[[CloseIfAbrupt]]', function (completion) {
			closeCalledWith = completion;
		});

		var result = GeneratorResumeAbrupt(gen, ReturnCompletion(99), 'test');
		st.ok(closeCalledWith instanceof CompletionRecord, 'closeIfAbrupt called with CompletionRecord');
		st.equal(closeCalledWith.type(), 'return', 'completion is a return');
		st.equal(closeCalledWith.value(), 99, 'completion value is 99');
		st.deepEqual(result, { value: 99, done: true }, 'returns {value: 99, done: true}');
		st.equal(SLOT.get(gen, '[[GeneratorState]]'), 'COMPLETED', 'state is COMPLETED');
		st.end();
	});

	t.end();
});

test('GetOptionsObject', function (t) {
	t.test('undefined returns null-prototype object', function (st) {
		var result = GetOptionsObject(void undefined);
		st.equal(typeof result, 'object', 'returns an object');
		st.equal(Object.getPrototypeOf(result), null, 'prototype is null');
		st.end();
	});

	t.test('object returns itself', function (st) {
		var obj = { a: 1 };
		st.equal(GetOptionsObject(obj), obj, 'returns the same object');
		st.end();
	});

	t.test('non-object, non-undefined throws TypeError', function (st) {
		forEach(v.primitives, function (primitive) {
			if (typeof primitive !== 'undefined') {
				st['throws'](
					function () { GetOptionsObject(primitive); },
					TypeError,
					debug(primitive) + ' throws TypeError'
				);
			}
		});
		st.end();
	});

	t.end();
});

test('IfAbruptCloseIterators', function (t) {
	t.test('normal completion returns value', function (st) {
		var result = IfAbruptCloseIterators(NormalCompletion(42), []);
		st.equal(result, 42, 'returns unwrapped value');
		st.end();
	});

	t.test('throw completion closes iterators and throws', function (st) {
		var closeCalled = false;
		var iterRecord = {
			'[[Iterator]]': {
				next: function () { return { done: true }; },
				'return': function () { closeCalled = true; return { done: true }; }
			},
			'[[NextMethod]]': function () { return { done: true }; },
			'[[Done]]': false
		};

		st['throws'](
			function () { IfAbruptCloseIterators(ThrowCompletion(new EvalError('test')), [iterRecord]); },
			EvalError,
			'throws the error'
		);
		st.equal(closeCalled, true, 'iterator was closed');
		st.end();
	});

	t.test('non-CompletionRecord throws TypeError', function (st) {
		st['throws'](
			function () { IfAbruptCloseIterators(42, []); },
			TypeError,
			'non-CompletionRecord throws'
		);
		st.end();
	});

	t.end();
});

test('IteratorCloseAll', function (t) {
	t.test('closes all iterators in reverse order', function (st) {
		var closed = [];
		var makeIter = function (name) {
			return {
				'[[Iterator]]': {
					next: function () { return { done: true }; },
					'return': function () { closed.push(name); return { done: true }; }
				},
				'[[NextMethod]]': function () { return { done: true }; },
				'[[Done]]': false
			};
		};

		st['throws'](
			function () { IteratorCloseAll([makeIter('a'), makeIter('b'), makeIter('c')], ThrowCompletion(new EvalError('test'))); },
			EvalError,
			'throws the completion'
		);
		st.deepEqual(closed, ['c', 'b', 'a'], 'closed in reverse order');
		st.end();
	});

	t.test('close error replaces normal completion', function (st) {
		var iter = {
			'[[Iterator]]': {
				next: function () { return { done: true }; },
				'return': function () { throw new SyntaxError('close error'); }
			},
			'[[NextMethod]]': function () { return { done: true }; },
			'[[Done]]': false
		};

		st['throws'](
			function () { IteratorCloseAll([iter], NormalCompletion(void undefined)); },
			SyntaxError,
			'close error replaces normal completion'
		);
		st.end();
	});

	t.test('original throw completion wins over close error', function (st) {
		var iter = {
			'[[Iterator]]': {
				next: function () { return { done: true }; },
				'return': function () { throw new SyntaxError('close error'); }
			},
			'[[NextMethod]]': function () { return { done: true }; },
			'[[Done]]': false
		};

		st['throws'](
			function () { IteratorCloseAll([iter], ThrowCompletion(new EvalError('original'))); },
			EvalError,
			'original throw completion takes precedence'
		);
		st.end();
	});

	t.test('normal completion after successful close returns undefined', function (st) {
		var iter = {
			'[[Iterator]]': {
				next: function () { return { done: true }; },
				'return': function () { return { done: true }; }
			},
			'[[NextMethod]]': function () { return { done: true }; },
			'[[Done]]': false
		};

		var result = IteratorCloseAll([iter], NormalCompletion(void undefined));
		st.equal(result, void undefined, 'returns undefined');
		st.end();
	});

	t.test('validation', function (st) {
		st['throws'](
			function () { IteratorCloseAll('not an array', NormalCompletion(void undefined)); },
			TypeError,
			'non-array iters throws'
		);
		st['throws'](
			function () { IteratorCloseAll([], 'not a completion'); },
			TypeError,
			'non-CompletionRecord throws'
		);
		st.end();
	});

	t.end();
});
