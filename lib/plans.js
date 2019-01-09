'use strict';

var states = {
  created: 'created',
  retry: 'retry',
  active: 'active',
  completed: 'completed',
  expired: 'expired',
  cancelled: 'cancelled',
  failed: 'failed'
};

var completedJobPrefix = '__state__' + states.completed + '__';

module.exports = {
  create: create,
  insertVersion: insertVersion,
  getVersion: getVersion,
  versionTableExists: versionTableExists,
  fetchNextJob: fetchNextJob,
  completeJobs: completeJobs,
  cancelJobs: cancelJobs,
  failJobs: failJobs,
  insertJob: insertJob,
  expire: expire,
  archive: archive,
  purge: purge,
  countStates: countStates,
  deleteQueue: deleteQueue,
  deleteAllQueues: deleteAllQueues,
  states: Object.assign({}, states),
  completedJobPrefix: completedJobPrefix
};

function create(schema) {
  return [createSchema(schema), tryCreateCryptoExtension(), createVersionTable(schema), createJobStateEnum(schema), createJobTable(schema), cloneJobTableForArchive(schema), addArchivedOnToArchive(schema), createIndexJobName(schema), createIndexSingletonOn(schema), createIndexSingletonKeyOn(schema), createIndexSingletonKey(schema)];
}

function createSchema(schema) {
  return '\n    CREATE SCHEMA IF NOT EXISTS ' + schema + '\n  ';
}

function tryCreateCryptoExtension() {
  return '\n    CREATE EXTENSION IF NOT EXISTS pgcrypto\n  ';
}

function createVersionTable(schema) {
  return '\n    CREATE TABLE ' + schema + '.version (\n      version text primary key\n    )\n  ';
}

function createJobStateEnum(schema) {
  // ENUM definition order is important
  // base type is numeric and first values are less than last values
  return '\n    CREATE TYPE ' + schema + '.job_state AS ENUM (\n      \'' + states.created + '\',\n      \'' + states.retry + '\',\n      \'' + states.active + '\',\t\n      \'' + states.completed + '\',\n      \'' + states.expired + '\',\n      \'' + states.cancelled + '\',\n      \'' + states.failed + '\'\n    )\n  ';
}

function createJobTable(schema) {
  return '\n    CREATE TABLE ' + schema + '.job (\n      id uuid primary key not null default gen_random_uuid(),\n      name text not null,\n      priority integer not null default(0),\n      data jsonb,\n      state ' + schema + '.job_state not null default(\'' + states.created + '\'),\n      retryLimit integer not null default(0),\n      retryCount integer not null default(0),\n      retryDelay integer not null default(0),\n      retryBackoff boolean not null default false,\n      startAfter timestamp with time zone not null default now(),\n      startedOn timestamp with time zone,\n      singletonKey text,\n      singletonOn timestamp without time zone,\n      expireIn interval not null default interval \'15 minutes\',\n      createdOn timestamp with time zone not null default now(),\n      completedOn timestamp with time zone\n    )\n  ';
}

function cloneJobTableForArchive(schema) {
  return 'CREATE TABLE ' + schema + '.archive (LIKE ' + schema + '.job)';
}

function addArchivedOnToArchive(schema) {
  return 'ALTER TABLE ' + schema + '.archive ADD archivedOn timestamptz NOT NULL DEFAULT now()';
}

function deleteQueue(schema) {
  return 'DELETE FROM ' + schema + '.job WHERE name = $1';
}

function deleteAllQueues(schema) {
  return 'TRUNCATE ' + schema + '.job';
}

function createIndexSingletonKey(schema) {
  // anything with singletonKey means "only 1 job can be queued or active at a time"
  return '\n    CREATE UNIQUE INDEX job_singletonKey ON ' + schema + '.job (name, singletonKey) WHERE state < \'' + states.completed + '\' AND singletonOn IS NULL\n  ';
}

function createIndexSingletonOn(schema) {
  // anything with singletonOn means "only 1 job within this time period, queued, active or completed"
  return '\n    CREATE UNIQUE INDEX job_singletonOn ON ' + schema + '.job (name, singletonOn) WHERE state < \'' + states.expired + '\' AND singletonKey IS NULL\n  ';
}

function createIndexSingletonKeyOn(schema) {
  // anything with both singletonOn and singletonKey means "only 1 job within this time period with this key, queued, active or completed"
  return '\n    CREATE UNIQUE INDEX job_singletonKeyOn ON ' + schema + '.job (name, singletonOn, singletonKey) WHERE state < \'' + states.expired + '\'\n  ';
}

function createIndexJobName(schema) {
  return '\n    CREATE INDEX job_name ON ' + schema + '.job (name text_pattern_ops)\n  ';
}

function getVersion(schema) {
  return '\n    SELECT version from ' + schema + '.version\n  ';
}

function versionTableExists(schema) {
  return '\n    SELECT to_regclass(\'' + schema + '.version\') as name\n  ';
}

function insertVersion(schema) {
  return '\n    INSERT INTO ' + schema + '.version(version) VALUES ($1)\n  ';
}

function fetchNextJob(schema) {
  return '\n    WITH nextJob as (\n      SELECT id\n      FROM ' + schema + '.job\n      WHERE state < \'' + states.active + '\'\n        AND name LIKE $1\n        AND startAfter < now()\n      ORDER BY priority desc, createdOn, id\n      LIMIT $2\n      FOR UPDATE SKIP LOCKED\n    )\n    UPDATE ' + schema + '.job j SET\n      state = \'' + states.active + '\',\n      startedOn = now(),\n      retryCount = CASE WHEN state = \'' + states.retry + '\' THEN retryCount + 1 ELSE retryCount END\n    FROM nextJob\n    WHERE j.id = nextJob.id\n    RETURNING j.id, name, data\n  ';
}

function buildJsonCompletionObject(withResponse) {
  // job completion contract
  return 'jsonb_build_object(\n    \'request\', jsonb_build_object(\'id\', id, \'name\', name, \'data\', data),\n    \'response\', ' + (withResponse ? '$2::jsonb' : 'null') + ',\n    \'state\', state,\n    \'retryCount\', retryCount,\n    \'createdOn\', createdOn,\n    \'startedOn\', startedOn,\n    \'completedOn\', completedOn,\n    \'failed\', CASE WHEN state = \'' + states.completed + '\' THEN false ELSE true END\n  )';
}

var retryCompletedOnCase = 'CASE\n          WHEN retryCount < retryLimit\n          THEN NULL\n          ELSE now()\n          END';

var retryStartAfterCase = 'CASE\n          WHEN retryCount = retryLimit THEN startAfter\n          WHEN NOT retryBackoff THEN now() + retryDelay * interval \'1\'\n          ELSE now() +\n            (\n                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2\n                +\n                retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2 * random()\n            )\n            * interval \'1\'\n          END';

function completeJobs(schema) {
  return '\n    WITH results AS (\n      UPDATE ' + schema + '.job\n      SET completedOn = now(),\n        state = \'' + states.completed + '\'\n      WHERE id IN (SELECT UNNEST($1::uuid[]))\n        AND state = \'' + states.active + '\'\n      RETURNING *\n    )\n    INSERT INTO ' + schema + '.job (name, data)\n    SELECT\n      \'' + completedJobPrefix + '\' || name, \n      ' + buildJsonCompletionObject(true) + '\n    FROM results\n    WHERE NOT name LIKE \'' + completedJobPrefix + '%\'\n    RETURNING 1\n  '; // returning 1 here just to count results against input array
}

function failJobs(schema) {
  return '\n    WITH results AS (\n      UPDATE ' + schema + '.job\n      SET state = CASE\n          WHEN retryCount < retryLimit\n          THEN \'' + states.retry + '\'::' + schema + '.job_state\n          ELSE \'' + states.failed + '\'::' + schema + '.job_state\n          END,        \n        completedOn = ' + retryCompletedOnCase + ',\n        startAfter = ' + retryStartAfterCase + '\n      WHERE id IN (SELECT UNNEST($1::uuid[]))\n        AND state < \'' + states.completed + '\'\n      RETURNING *\n    )\n    INSERT INTO ' + schema + '.job (name, data)\n    SELECT\n      \'' + completedJobPrefix + '\' || name,\n      ' + buildJsonCompletionObject(true) + '\n    FROM results\n    WHERE state = \'' + states.failed + '\'\n      AND NOT name LIKE \'' + completedJobPrefix + '%\'\n    RETURNING 1\n  '; // returning 1 here just to count results against input array
}

function expire(schema) {
  return '\n    WITH results AS (\n      UPDATE ' + schema + '.job\n      SET state = CASE\n          WHEN retryCount < retryLimit THEN \'' + states.retry + '\'::' + schema + '.job_state\n          ELSE \'' + states.expired + '\'::' + schema + '.job_state\n          END,        \n        completedOn = ' + retryCompletedOnCase + ',\n        startAfter = ' + retryStartAfterCase + '\n      WHERE state = \'' + states.active + '\'\n        AND (startedOn + expireIn) < now()    \n      RETURNING *\n    )\n    INSERT INTO ' + schema + '.job (name, data)\n    SELECT\n      \'' + completedJobPrefix + '\' || name,\n      ' + buildJsonCompletionObject() + '\n    FROM results\n    WHERE state = \'' + states.expired + '\'\n      AND NOT name LIKE \'' + completedJobPrefix + '%\'\n  ';
}

function cancelJobs(schema) {
  return '\n    UPDATE ' + schema + '.job\n    SET completedOn = now(),\n      state = \'' + states.cancelled + '\'\n    WHERE id IN (SELECT UNNEST($1::uuid[]))\n      AND state < \'' + states.completed + '\'\n    RETURNING 1\n  '; // returning 1 here just to count results against input array
}

function insertJob(schema) {
  return '\n    INSERT INTO ' + schema + '.job (\n      id, \n      name, \n      priority, \n      state, \n      retryLimit, \n      startAfter, \n      expireIn, \n      data, \n      singletonKey, \n      singletonOn,\n      retryDelay, \n      retryBackoff\n      )\n    VALUES (\n      $1,\n      $2,\n      $3,\n      \'' + states.created + '\',\n      $4, \n      CASE WHEN right($5, 1) = \'Z\' THEN CAST($5 as timestamp with time zone) ELSE now() + CAST(COALESCE($5,\'0\') as interval) END,\n      CAST($6 as interval),\n      $7,\n      $8,\n      CASE WHEN $9::integer IS NOT NULL THEN \'epoch\'::timestamp + \'1 second\'::interval * ($9 * floor((date_part(\'epoch\', now()) + $10) / $9)) ELSE NULL END,\n      $11,\n      $12\n    )\n    ON CONFLICT DO NOTHING\n    RETURNING 1\n  '; // returning 1 here so we can actually return id on publish
}

function purge(schema) {
  return '\n    DELETE FROM ' + schema + '.archive\n    WHERE (archivedOn + CAST($1 as interval) < now())\n  ';
}

function archive(schema) {
  return '\n    WITH archived_rows AS (\n      DELETE FROM ' + schema + '.job\n      WHERE\n        (completedOn + CAST($1 as interval) < now())\n        OR (\n          state = \'' + states.created + '\'\n          AND name LIKE \'' + completedJobPrefix + '%\'\n          AND createdOn + CAST($1 as interval) < now()\n        )\n      RETURNING *\n    )\n    INSERT INTO ' + schema + '.archive (\n      id, name, priority, data, state, retryLimit, retryCount, retryDelay, retryBackoff, startAfter, startedOn, singletonKey, singletonOn, expireIn, createdOn, completedOn\n    )\n    SELECT \n      id, name, priority, data, state, retryLimit, retryCount, retryDelay, retryBackoff, startAfter, startedOn, singletonKey, singletonOn, expireIn, createdOn, completedOn\n    FROM archived_rows\n  ';
}

function countStates(schema) {
  return '\n    SELECT name, state, count(*) size\n    FROM ' + schema + '.job\n    WHERE name NOT LIKE \'' + completedJobPrefix + '%\'\n    GROUP BY rollup(name), rollup(state)\n  ';
}