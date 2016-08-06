// market depth keeps a local copy of the orderbook

var util = require(__dirname + '/../util');
var log = require(util.dirs().core + 'log');


var _ = require('lodash');
var OPERATIONS = {Insert : 1, Update : 2, Delete : 3};

var MarketDepth = function() {
  this.bids = new Array();
  this.asks = new Array();
  this.balances = new Array();
  this.averageBalance = 0;
  this.midPointPrice = 0;
  this.volume = 0;
  this.previousVolume = 0;
  this.cumulativeBid = 0;
  this.cumulativeAsk = 0;

  _.bindAll(this);
}

// util.makeEventEmitter(Heart);

MarketDepth.prototype.getSizes = function() {
  return this.cumulativeBid + "-" + this.cumulativeAsk;
}

MarketDepth.prototype.getTop = function() {
  return _.last(this.bids).price + "-" + _.last(this.asks).price;

}

MarketDepth.prototype.reset = function() {
  this.bids = new Array();
  this.asks = new Array();
  this.balances = new Array();
}

MarketDepth.prototype.getCumulativeSize = function(items) {
  return _.reduce(items, (a,b) => a.size + b.size, 0);
}

MarketDepth.prototype.update = function(position, operation, side, price, size) {
  var items = (side == 'bids') ? this.bids : this.asks;
  var levels = _.size(items);
  switch(operation) {
    case OPERATIONS.Insert:
      if (position <= levels)
        items.add(position, {'size': size, 'price': price});
      break;

    case OPERATIONS.Update:
      if (position < levels) {
        var item = items.get(position);
        item.setSize(size);
        item.setPrice(price);
      }
      break;

    case OPERATIONS.Delete:
      if (position < levels)
        items.remove(position);
      break;
  }

  if (operation == OPERATIONS.Update) {
    if (!bids.isEmpty() && !asks.isEmpty()) {
      cumulativeBid = getCumulativeSize(bids);
      cumulativeAsk = getCumulativeSize(asks);

      double totalDepth = cumulativeBid + cumulativeAsk;
      if (totalDepth != 0) {
          double balance = 100.0d * (cumulativeBid - cumulativeAsk) / totalDepth;
          balances.add(balance);
          midPointPrice = (bids.getFirst().getPrice() + asks.getFirst().getPrice()) / 2;
      }
    }
  }
}

module.exports = {
  MarketDepth: MarketDepth, 
  OPERATIONS: OPERATIONS
};
//TODO: write test cases