'use strict';

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; };

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var assert = require('assert');
var EventEmitter = require('events');
var Promise = require('bluebird');

var Worker = require('./worker');
var plans = require('./plans');
var Attorney = require('./attorney');

var completedJobPrefix = plans.completedJobPrefix;

var events = {
  error: 'error'
};

var Manager = function (_EventEmitter) {
  _inherits(Manager, _EventEmitter);

  function Manager(db, config) {
    _classCallCheck(this, Manager);

    var _this = _possibleConstructorReturn(this, (Manager.__proto__ || Object.getPrototypeOf(Manager)).call(this));

    _this.config = config;
    _this.db = db;

    _this.events = events;
    _this.subscriptions = {};

    _this.nextJobCommand = plans.fetchNextJob(config.schema);
    _this.insertJobCommand = plans.insertJob(config.schema);
    _this.completeJobsCommand = plans.completeJobs(config.schema);
    _this.cancelJobsCommand = plans.cancelJobs(config.schema);
    _this.failJobsCommand = plans.failJobs(config.schema);
    _this.deleteQueueCommand = plans.deleteQueue(config.schema);
    _this.deleteAllQueuesCommand = plans.deleteAllQueues(config.schema);

    // exported api to index
    _this.functions = [_this.fetch, _this.complete, _this.cancel, _this.fail, _this.publish, _this.subscribe, _this.unsubscribe, _this.onComplete, _this.offComplete, _this.fetchCompleted, _this.publishDebounced, _this.publishThrottled, _this.publishOnce, _this.publishAfter, _this.deleteQueue, _this.deleteAllQueues];
    return _this;
  }

  _createClass(Manager, [{
    key: 'stop',
    value: function stop() {
      var _this2 = this;

      Object.keys(this.subscriptions).forEach(function (name) {
        return _this2.unsubscribe(name);
      });
      this.subscriptions = {};
      return Promise.resolve(true);
    }
  }, {
    key: 'subscribe',
    value: function subscribe(name) {
      var _this3 = this;

      for (var _len = arguments.length, args = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
        args[_key - 1] = arguments[_key];
      }

      return Attorney.checkSubscribeArgs(name, args).then(function (_ref) {
        var options = _ref.options,
            callback = _ref.callback;
        return _this3.watch(name, options, callback);
      });
    }
  }, {
    key: 'onComplete',
    value: function onComplete(name) {
      var _this4 = this;

      for (var _len2 = arguments.length, args = Array(_len2 > 1 ? _len2 - 1 : 0), _key2 = 1; _key2 < _len2; _key2++) {
        args[_key2 - 1] = arguments[_key2];
      }

      return Attorney.checkSubscribeArgs(name, args).then(function (_ref2) {
        var options = _ref2.options,
            callback = _ref2.callback;
        return _this4.watch(completedJobPrefix + name, options, callback);
      });
    }
  }, {
    key: 'watch',
    value: function watch(name, options, callback) {
      var _this5 = this;

      // watch() is always nested in a promise, so assert()s are welcome

      if ('newJobCheckInterval' in options || 'newJobCheckIntervalSeconds' in options) options = Attorney.applyNewJobCheckInterval(options);else options.newJobCheckInterval = this.config.newJobCheckInterval;

      if ('teamConcurrency' in options) {
        var teamConcurrencyErrorMessage = 'teamConcurrency must be an integer between 1 and 1000';
        assert(Number.isInteger(options.teamConcurrency) && options.teamConcurrency >= 1 && options.teamConcurrency <= 1000, teamConcurrencyErrorMessage);
      }

      if ('teamSize' in options) {
        var teamSizeErrorMessage = 'teamSize must be an integer > 0';
        assert(Number.isInteger(options.teamSize) && options.teamSize >= 1, teamSizeErrorMessage);
      }

      if ('batchSize' in options) {
        var batchSizeErrorMessage = 'batchSize must be an integer > 0';
        assert(Number.isInteger(options.batchSize) && options.batchSize >= 1, batchSizeErrorMessage);
      }

      var sendItBruh = function sendItBruh(jobs) {
        if (!jobs) return Promise.resolve();

        // If you get a batch, for now you should use complete() so you can control
        //   whether individual or group completion responses apply to your use case
        // Failing will fail all fetched jobs
        if (options.batchSize) return Promise.all([callback(jobs)]).catch(function (err) {
          return _this5.fail(jobs.map(function (job) {
            return job.id;
          }), err);
        });

        // either no option was set, or teamSize was used
        return Promise.map(jobs, function (job) {
          return callback(job).then(function (value) {
            return _this5.complete(job.id, value);
          }).catch(function (err) {
            return _this5.fail(job.id, err);
          });
        }, { concurrency: options.teamConcurrency || 2 });
      };

      var onError = function onError(error) {
        return _this5.emit(events.error, error);
      };

      var workerConfig = {
        name: name,
        fetch: function fetch() {
          return _this5.fetch(name, options.batchSize || options.teamSize || 1);
        },
        onFetch: function onFetch(jobs) {
          return sendItBruh(jobs).catch(function (err) {
            return null;
          });
        }, // just send it, bruh
        onError: onError,
        interval: options.newJobCheckInterval
      };

      var worker = new Worker(workerConfig);
      worker.start();

      if (!this.subscriptions[name]) this.subscriptions[name] = { workers: [] };

      this.subscriptions[name].workers.push(worker);

      return Promise.resolve(true);
    }
  }, {
    key: 'unsubscribe',
    value: function unsubscribe(name) {
      if (!this.subscriptions[name]) return Promise.reject('No subscriptions for ' + name + ' were found.');

      this.subscriptions[name].workers.forEach(function (worker) {
        return worker.stop();
      });
      delete this.subscriptions[name];

      return Promise.resolve(true);
    }
  }, {
    key: 'offComplete',
    value: function offComplete(name) {
      return this.unsubscribe(completedJobPrefix + name);
    }
  }, {
    key: 'publish',
    value: function publish() {
      var _this6 = this;

      for (var _len3 = arguments.length, args = Array(_len3), _key3 = 0; _key3 < _len3; _key3++) {
        args[_key3] = arguments[_key3];
      }

      return Attorney.checkPublishArgs(args).then(function (_ref3) {
        var name = _ref3.name,
            data = _ref3.data,
            options = _ref3.options;
        return _this6.createJob(name, data, options);
      });
    }
  }, {
    key: 'publishOnce',
    value: function publishOnce(name, data, options, key) {
      var _this7 = this;

      return Attorney.checkPublishArgs([name, data, options]).then(function (_ref4) {
        var name = _ref4.name,
            data = _ref4.data,
            options = _ref4.options;


        options.singletonKey = key;

        return _this7.createJob(name, data, options);
      });
    }
  }, {
    key: 'publishAfter',
    value: function publishAfter(name, data, options, after) {
      var _this8 = this;

      return Attorney.checkPublishArgs([name, data, options]).then(function (_ref5) {
        var name = _ref5.name,
            data = _ref5.data,
            options = _ref5.options;


        options.startAfter = after;

        return _this8.createJob(name, data, options);
      });
    }
  }, {
    key: 'publishThrottled',
    value: function publishThrottled(name, data, options, seconds, key) {
      var _this9 = this;

      return Attorney.checkPublishArgs([name, data, options]).then(function (_ref6) {
        var name = _ref6.name,
            data = _ref6.data,
            options = _ref6.options;


        options.singletonSeconds = seconds;
        options.singletonNextSlot = false;
        options.singletonKey = key;

        return _this9.createJob(name, data, options);
      });
    }
  }, {
    key: 'publishDebounced',
    value: function publishDebounced(name, data, options, seconds, key) {
      var _this10 = this;

      return Attorney.checkPublishArgs([name, data, options]).then(function (_ref7) {
        var name = _ref7.name,
            data = _ref7.data,
            options = _ref7.options;


        options.singletonSeconds = seconds;
        options.singletonNextSlot = true;
        options.singletonKey = key;

        return _this10.createJob(name, data, options);
      });
    }
  }, {
    key: 'createJob',
    value: function createJob(name, data, options, singletonOffset) {
      var _this11 = this;

      var startAfter = options.startAfter;

      startAfter = startAfter instanceof Date && typeof startAfter.toISOString === 'function' ? startAfter.toISOString() : startAfter > 0 ? '' + startAfter : typeof startAfter === 'string' ? startAfter : null;

      if ('retryDelay' in options) assert(Number.isInteger(options.retryDelay) && options.retryDelay >= 0, 'retryDelay must be an integer >= 0');

      if ('retryBackoff' in options) assert(options.retryBackoff === true || options.retryBackoff === false, 'retryBackoff must be either true or false');

      if ('retryLimit' in options) assert(Number.isInteger(options.retryLimit) && options.retryLimit >= 0, 'retryLimit must be an integer >= 0');

      var retryLimit = options.retryLimit || 0;
      var retryBackoff = !!options.retryBackoff;
      var retryDelay = options.retryDelay || 0;

      if (retryBackoff && !retryDelay) retryDelay = 1;

      if (retryDelay && !retryLimit) retryLimit = 1;

      var expireIn = options.expireIn || '15 minutes';
      var priority = options.priority || 0;

      var singletonSeconds = options.singletonSeconds > 0 ? options.singletonSeconds : options.singletonMinutes > 0 ? options.singletonMinutes * 60 : options.singletonHours > 0 ? options.singletonHours * 60 * 60 : null;

      var singletonKey = options.singletonKey || null;

      singletonOffset = singletonOffset || 0;

      var id = require('uuid/' + this.config.uuid)();

      // ordinals! [1,  2,    3,        4,          5,          6,        7,    8,            9,                10,              11,         12          ]
      var values = [id, name, priority, retryLimit, startAfter, expireIn, data, singletonKey, singletonSeconds, singletonOffset, retryDelay, retryBackoff];

      return this.db.executeSql(this.insertJobCommand, values).then(function (result) {
        if (result.rowCount === 1) return id;

        if (!options.singletonNextSlot) return null;

        // delay starting by the offset to honor throttling config
        options.startAfter = singletonSeconds;
        // toggle off next slot config for round 2
        options.singletonNextSlot = false;

        var singletonOffset = singletonSeconds;

        return _this11.createJob(name, data, options, singletonOffset);
      });
    }
  }, {
    key: 'fetch',
    value: function fetch(name, batchSize) {
      var _this12 = this;

      return Attorney.checkFetchArgs(name, batchSize).then(function (values) {
        return _this12.db.executeSql(_this12.nextJobCommand, [values.name, values.batchSize || 1]);
      }).then(function (result) {

        var jobs = result.rows.map(function (job) {
          job.done = function (error, response) {
            return error ? _this12.fail(job.id, error) : _this12.complete(job.id, response);
          };
          return job;
        });

        return jobs.length === 0 ? null : jobs.length === 1 && !batchSize ? jobs[0] : jobs;
      });
    }
  }, {
    key: 'fetchCompleted',
    value: function fetchCompleted(name, batchSize) {
      return this.fetch(completedJobPrefix + name, batchSize);
    }
  }, {
    key: 'mapCompletionIdArg',
    value: function mapCompletionIdArg(id, funcName) {
      var errorMessage = funcName + '() requires an id';

      return Attorney.assertAsync(id, errorMessage).then(function () {
        var ids = Array.isArray(id) ? id : [id];
        assert(ids.length, errorMessage);
        return ids;
      });
    }
  }, {
    key: 'mapCompletionDataArg',
    value: function mapCompletionDataArg(data) {
      if (data === null || typeof data === 'undefined' || typeof data === 'function') return null;

      if (data instanceof Error) data = JSON.parse(JSON.stringify(data, Object.getOwnPropertyNames(data)));

      return (typeof data === 'undefined' ? 'undefined' : _typeof(data)) === 'object' && !Array.isArray(data) ? data : { value: data };
    }
  }, {
    key: 'mapCompletionResponse',
    value: function mapCompletionResponse(ids, result) {
      return {
        jobs: ids,
        requested: ids.length,
        updated: result.rowCount
      };
    }
  }, {
    key: 'complete',
    value: function complete(id, data) {
      var _this13 = this;

      return this.mapCompletionIdArg(id, 'complete').then(function (ids) {
        return _this13.db.executeSql(_this13.completeJobsCommand, [ids, _this13.mapCompletionDataArg(data)]).then(function (result) {
          return _this13.mapCompletionResponse(ids, result);
        });
      });
    }
  }, {
    key: 'fail',
    value: function fail(id, data) {
      var _this14 = this;

      return this.mapCompletionIdArg(id, 'fail').then(function (ids) {
        return _this14.db.executeSql(_this14.failJobsCommand, [ids, _this14.mapCompletionDataArg(data)]).then(function (result) {
          return _this14.mapCompletionResponse(ids, result);
        });
      });
    }
  }, {
    key: 'cancel',
    value: function cancel(id) {
      var _this15 = this;

      return this.mapCompletionIdArg(id, 'cancel').then(function (ids) {
        return _this15.db.executeSql(_this15.cancelJobsCommand, [ids]).then(function (result) {
          return _this15.mapCompletionResponse(ids, result);
        });
      });
    }
  }, {
    key: 'deleteQueue',
    value: function deleteQueue(queue) {
      return this.db.executeSql(this.deleteQueueCommand, [queue]);
    }
  }, {
    key: 'deleteAllQueues',
    value: function deleteAllQueues() {
      return this.db.executeSql(this.deleteAllQueuesCommand);
    }
  }]);

  return Manager;
}(EventEmitter);

module.exports = Manager;