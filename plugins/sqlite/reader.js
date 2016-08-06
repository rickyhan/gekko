var _ = require('lodash');
var util = require('../../core/util.js');
var config = util.getConfig();
var log = require(util.dirs().core + 'log');

var handle = require('./handle');
var sqliteUtil = require('./util');

var Reader = function() {
  _.bindAll(this);
  this.db = handle;
}

// == candles ==
// returns the furtherst point (up to `from`) in time we have valid data from
Reader.prototype.mostRecentWindow = function(to, from, next) {
  var maxAmount = ((to - from) / 60) + 1;

  this.db.all(`
    SELECT start from ${sqliteUtil.table('candles')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start DESC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error while reading mostRecentWindow');
    }

    if(rows.length === 0) {
      return next(false);
    }

    if(rows.length === maxAmount) {
      return next(from);
    }

    // we have a gap
    var gapIndex = _.findIndex(rows, function(r, i) {
      return r.start !== to - i * 60;
    });

    // if no candle is recent enough
    if(gapIndex === 0) {
      return next(false);
    }

    // if there was no gap in the records, but
    // there were not enough records.
    if(gapIndex === -1)
      gapIndex = rows.length;

    next(to - gapIndex * 60);
  })
}

Reader.prototype.get = function(from, to, what, next) {
  if(what === 'full')
    what = '*';

  this.db.all(`
    SELECT ${what} from ${sqliteUtil.table('candles')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start ASC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `get`');
    }

    next(null, rows);
  });
}

Reader.prototype.count = function(from, to, next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('candles')}
    WHERE start <= ${to} AND start >= ${from}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `count`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.countTotal = function(next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('candles')}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `countTotal`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.getBoundry = function(next) {

  this.db.all(`
    SELECT
    (
      SELECT start
      FROM ${sqliteUtil.table('candles')}
      ORDER BY start LIMIT 1
    ) as 'first',
    (
      SELECT start
      FROM ${sqliteUtil.table('candles')}
      ORDER BY start DESC
      LIMIT 1
    ) as 'last'
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `getBoundry`');
    }

    next(null, _.first(rows));
  });
}


// == trades ==
Reader.prototype.mostRecentWindowTrades = function(to, from, next) {
  var maxAmount = ((to - from) / 60) + 1;

  this.db.all(`
    SELECT start from ${sqliteUtil.table('trades')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start DESC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error while reading mostRecentWindowTrades');
    }

    if(rows.length === 0) {
      return next(false);
    }

    if(rows.length === maxAmount) {
      return next(from);
    }

    // we have a gap
    var gapIndex = _.findIndex(rows, function(r, i) {
      return r.start !== to - i * 60;
    });

    // if no candle is recent enough
    if(gapIndex === 0) {
      return next(false);
    }

    // if there was no gap in the records, but
    // there were not enough records.
    if(gapIndex === -1)
      gapIndex = rows.length;

    next(to - gapIndex * 60);
  })
}

Reader.prototype.getTrades = function(from, to, what, next) {
  if(what === 'full')
    what = '*';

  this.db.all(`
    SELECT ${what} from ${sqliteUtil.table('trades')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start ASC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `getTrades`');
    }

    next(null, rows);
  });
}

Reader.prototype.countTrades = function(from, to, next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('trades')}
    WHERE start <= ${to} AND start >= ${from}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `countTrades`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.countTotalTrades = function(next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('trades')}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `countTotalTrades`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.getBoundryTrades = function(next) {

  this.db.all(`
    SELECT
    (
      SELECT start
      FROM ${sqliteUtil.table('trades')}
      ORDER BY start LIMIT 1
    ) as 'first',
    (
      SELECT start
      FROM ${sqliteUtil.table('trades')}
      ORDER BY start DESC
      LIMIT 1
    ) as 'last'
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `getBoundryTrades`');
    }

    next(null, _.first(rows));
  });
}


// == mytrades ==
Reader.prototype.mostRecentWindowMyTrades = function(to, from, next) {
  var maxAmount = ((to - from) / 60) + 1;

  this.db.all(`
    SELECT start from ${sqliteUtil.table('mytrades')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start DESC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error while reading mostRecentWindowMyTrades');
    }

    if(rows.length === 0) {
      return next(false);
    }

    if(rows.length === maxAmount) {
      return next(from);
    }

    // we have a gap
    var gapIndex = _.findIndex(rows, function(r, i) {
      return r.start !== to - i * 60;
    });

    // if no candle is recent enough
    if(gapIndex === 0) {
      return next(false);
    }

    // if there was no gap in the records, but
    // there were not enough records.
    if(gapIndex === -1)
      gapIndex = rows.length;

    next(to - gapIndex * 60);
  })
}

Reader.prototype.getMyTrades = function(from, to, what, next) {
  if(what === 'full')
    what = '*';

  this.db.all(`
    SELECT ${what} from ${sqliteUtil.table('mytrades')}
    WHERE start <= ${to} AND start >= ${from}
    ORDER BY start ASC
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `getMyTrades`');
    }

    next(null, rows);
  });
}

Reader.prototype.countMyTrades = function(from, to, next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('mytrades')}
    WHERE start <= ${to} AND start >= ${from}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `countMyTrades`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.countTotalMyTrades = function(next) {
  this.db.all(`
    SELECT COUNT(*) as count from ${sqliteUtil.table('mytrades')}
  `, function(err, res) {
    if(err) {
      console.error(err);
      return util.die('DB error at `countTotalMyTrades`');
    }

    next(null, _.first(res).count);
  });
}

Reader.prototype.getBoundryMyTrades = function(next) {

  this.db.all(`
    SELECT
    (
      SELECT start
      FROM ${sqliteUtil.table('mytrades')}
      ORDER BY start LIMIT 1
    ) as 'first',
    (
      SELECT start
      FROM ${sqliteUtil.table('mytrades')}
      ORDER BY start DESC
      LIMIT 1
    ) as 'last'
  `, function(err, rows) {
    if(err) {
      console.error(err);
      return util.die('DB error at `getBoundryMyTrades`');
    }

    next(null, _.first(rows));
  });
}


// == misc ==
Reader.prototype.close = function() {
  this.db = null;
}

module.exports = Reader;