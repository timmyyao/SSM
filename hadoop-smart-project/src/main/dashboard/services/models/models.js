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
angular.module('org.apache.hadoop.ssm.models', [])

  .factory('models', ['$timeout', 'conf', 'restapi', 'locator',
    function ($timeout, conf, restapi, locator) {
      'use strict';

      var util = {
        usage: function (current, total) {
          return total > 0 ? 100 * current / total : 0;
        },
        getOrCreate: function (obj, prop, init) {
          if (!obj.hasOwnProperty(prop)) {
            obj[prop] = init;
          }
          return obj[prop];
        },
        parseIntFromQueryPathTail: function (path) {
          return Number(_.last(path.split('.')).replace(/[^0-9]/g, ''));
        }
      };

      /**
       * Retrieves a model from backend as a promise.
       * The resolved object will have two special methods.
       *   `$subscribe` - watch model changes within a scope.
       *   `$data` - return pure model data without these two methods.
       */
      function get(path, decodeFn, args) {
        args = args || {};
        return restapi.get(path).then(function (response) {
          var oldModel;
          var model = decodeFn(response.data, args);

          model.$subscribe = function (scope, onData, onError) {
            restapi.subscribe(args.pathOverride || path, scope, function (data) {
              try {
                var newModel = decodeFn(data, args);
                if (!_.isEqual(newModel, oldModel)) {
                  oldModel = newModel;
                  return onData(newModel);
                }
              } catch (ex) {
                if (angular.isFunction(onError)) {
                  return onError(data);
                }
              }
            }, args.period);
          };

          model.$data = function () {
            return _.omit(model, _.isFunction);
          };

          return model;
        });
      }

      var decoder = {
        _asAssociativeArray: function (objs, decodeFn, keyName) {
          var result = {};
          _.map(objs, function (obj) {
            var model = decodeFn(obj);
            var key = model[keyName];
            result[key] = model;
          });
          return result;
        },
        rules: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.ruleSummary, 'id');
        },
        ruleSummary: function (obj) {
          return angular.merge(obj, {
            // extra properties
            isRunning: (obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            isDead: !(obj.state === 'ACTIVE' || obj.state === 'DRYRUN'),
            // extra methods
            pageUrl: locator.rule(obj.id),
            start: function () {
              return restapi.startRule(obj.id);
            },
            terminate: function () {
              return restapi.stopRule(obj.id);
            }
          });
        },
        rule: function (obj) {
          angular.merge(obj, {
            status: 'Active',
            //Todo: replace real name
            ruleName: 'Rule1',
            isRunning: true,
          });
          return obj;
        },
        /** Return an array of application alerts */
        ruleAlerts: function (obj) {
          if (obj.time > 0) {
            return [{
              severity: 'error',
              time: Number(obj.time),
              message: obj.error
            }];
          }
          return [];
        },
        ruleCommands: function (objs) {
          return decoder._asAssociativeArray(objs, decoder.command, 'cid');
        },
        command: function (obj) {
          return angular.merge(obj, {
            // extra properties
            isRunning: obj.state === 'EXECUTING',
             // extra methods
            pageUrl: locator.command(obj.rid, obj.cid)
          });
        }
      };

      var getter = {
        rules: function () {
          return get('rulelist', decoder.rules);
        },
        rule: function (ruleId) {
          return get('rules/' + ruleId + '/detail', decoder.rule);
        },
        ruleAlerts: function (ruleId) {
          return get('rules/' + ruleId + '/errors', decoder.ruleAlerts);
        },
        ruleCommands: function (ruleId) {
          return get('rules/' + ruleId + '/commands', decoder.ruleCommands);
        }
      };

      return {
        $get: getter,
        /** Attempts to get model and then subscribe changes as long as the scope is valid. */
        $subscribe: function (scope, getModelFn, onData, period) {
          var shouldCancel = false;
          var promise;
          scope.$on('$destroy', function () {
            shouldCancel = true;
            $timeout.cancel(promise);
          });
          function trySubscribe() {
            if (shouldCancel) {
              return;
            }
            getModelFn().then(function (data) {
              return onData(data);
            }, /*onerror=*/function () {
              promise = $timeout(trySubscribe, period || conf.restapiQueryInterval);
            });
          }

          trySubscribe();
        },
        DAG_DEATH_UNSPECIFIED: '9223372036854775807' /* Long.max */
      };
    }])
;
