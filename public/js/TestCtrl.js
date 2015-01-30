angular.module("mbd.testCtrl", []).controller('TestCtrl', [
  '$scope', '$http', function($scope, $http) {
    var sentimentCalc, start, step;
    $scope.matches = [];
    sentimentCalc = [];
    $scope.matches = [];
    $scope.events = [];
    $scope.lines = [];
    $http.get('/data/worldcup-matches.json').success(function(data, status) {
      $scope.matches = data;
      $scope.match = data[0];
      $scope.home_events = data[0].home_team_events;
      $scope.away_events = data[0].away_team_events;
      angular.forEach($scope.home_events, function(val, key) {
        $scope.lines.push({
          value: Number(val.time),
          text: val.type_of_event,
          "class": 'home'
        });
      });
      return angular.forEach($scope.away_events, function(val, key) {
        $scope.lines.push({
          value: Number(val.time),
          text: val.type_of_event,
          "class": 'away'
        });
      });
    });
    $http.get('/data/Brazil-Croatia.json').success(function(data, status) {
      var firstDate, graphData, minute, sentimentComb;
      sentimentComb = [];
      firstDate = null;
      minute = null;
      angular.forEach(data, function(val, key) {
        var combination, thisDate;
        thisDate = moment(val[1]);
        if (firstDate === null) {
          firstDate = thisDate.clone();
          minute = 0;
        } else {
          minute = thisDate.diff(firstDate, 'minutes');
        }
        if (val[4] <= 0) {
          combination = {
            minute: minute,
            date: moment().format(val[1]),
            sentiment: val[4]
          };
          sentimentComb.push(combination);
        }
      });
      sentimentCalc = _.transform(sentimentComb, function(res, n, key) {
        if (res[n.minute]) {
          res[n.minute] = res[n.minute] + Math.abs(n.sentiment);
        } else {
          res[n.minute] = Math.abs(n.sentiment);
        }
      });
      graphData = _.map($scope.labels, function(v) {
        if (sentimentCalc[v]) {
          return sentimentCalc[v];
        }
        return 0;
      });
      console.log(graphData);
      c3.generate({
        tooltip: {
          show: false
        },
        point: {
          show: false
        },
        bindto: "#matchChart",
        legend: {
          show: false
        },
        data: {
          type: 'spline',
          x: "x",
          json: {
            x: $scope.labels,
            sentiment: graphData
          }
        },
        axis: {
          x: {
            tick: {
              values: $scope.ticks
            }
          }
        },
        grid: {
          x: {
            lines: $scope.lines
          }
        }
      });
    });
    $scope.labels = _.range([start = 0], 99, [step = 1]);
    $scope.ticks = _.range([start = 0], 99, [step = 5]);
    $scope.chart = null;
  }
]);
