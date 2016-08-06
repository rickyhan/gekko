var _ = require('lodash');
var config = require('../../core/util.js').getConfig();

var handle = require('./handle');
var sqliteUtil = require('./util');

var Store = function(done, pluginMeta) {
  _.bindAll(this);
  this.done = done;

  this.db = handle;
  this.db.serialize(this.upsertTables);

  this.cache = [];
  this.trades = [];
  this.mytrades = [];
}
Store.prototype.upsertTables = function() {
  var createQueries = [
    `
      CREATE TABLE IF NOT EXISTS
      ${sqliteUtil.table('candles')} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start INTEGER UNIQUE,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        vwp REAL NOT NULL,
        volume REAL NOT NULL,
        trades INTERGER NOT NULL
      );
    `,

    `
      CREATE TABLE IF NOT EXISTS
      ${sqliteUtil.table('trades')} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start INTEGER UNIQUE,
        isBuy BOOL NOT NULL,
        amount REAL NOT NULL,
        price REAL NOT NULL
      );
    `,

    `
      CREATE TABLE IF NOT EXISTS
      ${sqliteUtil.table('mytrades')} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start INTEGER UNIQUE,
        isBuy BOOL NOT NULL,
        amount REAL NOT NULL,
        price REAL NOT NULL
      );
    `,

    `
      CREATE TABLE IF NOT EXISTS
      ${sqliteUtil.table('orderbookupdates')} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        start INTEGER UNIQUE,
        isModifyOrRemove BOOL NOT NULL,
        isBid BOOL NOT NULL,
        amount REAL ALLOW NULL,
        rate REAL NOT NULL
      );
    `,
    // TODO: create advices
    // ``
  ];

  var next = _.after(_.size(createQueries), this.done);

  _.each(createQueries, function(q) {
    this.db.run(q, next);
  }, this);
}

// == candles ==

Store.prototype.writeCandles = function() {
  if(_.isEmpty(this.cache))
    return;

  var stmt = this.db.prepare(`
    INSERT OR IGNORE INTO ${sqliteUtil.table('candles')}
    VALUES (?,?,?,?,?,?,?,?,?)
  `);

  _.each(this.cache, candle => {
    stmt.run(
      null,
      candle.start.unix(),
      candle.open,
      candle.high,
      candle.low,
      candle.close,
      candle.vwp,
      candle.volume,
      candle.trades
    );
  });

  stmt.finalize();

  this.cache = [];
}
var processCandle = function(candle, done) {

  // because we might get a lot of candles
  // in the same tick, we rather batch them
  // up and insert them at once at next tick.
  this.cache.push(candle);
  _.defer(this.writeCandles);

  // NOTE: sqlite3 has it's own buffering, at
  // this point we are confident that the candle will
  // get written to disk on next tick.
  done();
}

// == trades ==

Store.prototype.writeTrades = function() {
  if(_.isEmpty(this.trades))
    return;

  var stmt = this.db.prepare(`
    INSERT OR IGNORE INTO ${sqliteUtil.table('trades')}
    VALUES (?,?,?,?,?)
  `);

  _.each(this.trades, trade => {
    stmt.run(
      null,
      trade.start.unix(),
      trade.type,
      trade.amount,
      trade.price
    );
  });

  stmt.finalize();

  this.trades = [];
}
var processTrades = function(trade, done) {

  // because we might get a lot of candles
  // in the same tick, we rather batch them
  // up and insert them at once at next tick.
  this.trades.push(trade);
  _.defer(this.writeTrades);

  // NOTE: sqlite3 has it's own buffering, at
  // this point we are confident that the candle will
  // get written to disk on next tick.
  done();
}

// == mytrades ==

Store.prototype.writeMyTrades = function() {
  if(_.isEmpty(this.mytrades))
    return;

  var stmt = this.db.prepare(`
    INSERT OR IGNORE INTO ${sqliteUtil.table('mytrades')}
    VALUES (?,?,?,?,?)
  `);

  _.each(this.mytrades, mytrade => {
    stmt.run(
      null,
      mytrade.start.unix(),
      mytrade.type,
      mytrade.amount,
      mytrade.price
    );
  });

  stmt.finalize();

  this.mytrades = [];
}
var processMyTrades = function(mytrade, done) {

  // because we might get a lot of candles
  // in the same tick, we rather batch them
  // up and insert them at once at next tick.
  this.trades.push(mytrade);
  _.defer(this.writeMyTrades);

  // NOTE: sqlite3 has it's own buffering, at
  // this point we are confident that the candle will
  // get written to disk on next tick.
  done();
}

// == orderbookupdates ==

Store.prototype.writeOrderbookUpdates = function() {
  if(_.isEmpty(this.orderbookupdates))
    return;

  var stmt = this.db.prepare(`
    INSERT OR IGNORE INTO ${sqliteUtil.table('orderbookupdates')}
    VALUES (?,?,?,?,?)
  `);

  _.each(this.orderbookupdates, orderbookupdate => {
    stmt.run(
      null,
      mytrade.start.unix(),
      mytrade.isModifyOrRemove,
      mytrade.isBid,
      mytrade.amount,
      mytrade.rate
    );
  });

  stmt.finalize();

  this.orderbookupdates = [];
}
var processOrderbookUpdates = function(orderbookupdate, done) {

  // because we might get a lot of candles
  // in the same tick, we rather batch them
  // up and insert them at once at next tick.
  this.orderbookupdates.push(orderbookupdate);
  _.defer(this.writeOrderbookUpdates);

  // NOTE: sqlite3 has it's own buffering, at
  // this point we are confident that the candle will
  // get written to disk on next tick.
  done();
}

if(config.candleWriter.enabled)
  Store.prototype.processCandle = processCandle;

if(config.tradeWriter.enabled)
 Store.prototype.processTrades = processTrades;

if(config.myTradeWriter.enabled)
 Store.prototype.processMyTrades = processMyTrades;

// TODO: add storing of advice?

// var processAdvice = function(candles) {
//   util.die('NOT IMPLEMENTED');
// }

// if(config.adviceWriter.enabled)
//   Store.prototype.processAdvice = processAdvice;

module.exports = Store;