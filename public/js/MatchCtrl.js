angular.module("mbd.matchCtrl", ['chart.js']).controller('MatchCtrl', [
  '$scope', '$http', '$stateParams', function($scope, $http, $stateParams) {
    var start, step;
    $scope.matches = [];
    $scope.events = [];
    $scope.lines = [];
    $scope.error = null;
    $http.get('/public/data/worldcup-matches.json').success(function(data, status) {
      $scope.matches = data;
      $scope.match = data[$stateParams.index];
      $scope.home_events = data[$stateParams.index].home_team_events;
      $scope.away_events = data[$stateParams.index].away_team_events;
      angular.forEach($scope.home_events, function(val, key) {
        $scope.lines.push({
          value: Number(val.time),
          text: val.type_of_event,
          "class": 'home'
        });
      });
      angular.forEach($scope.away_events, function(val, key) {
        $scope.lines.push({
          value: Number(val.time),
          text: val.type_of_event,
          "class": 'away'
        });
      });
      return $http.get('/public/data/' + $scope.match.home_team.country + '-' + $scope.match.away_team.country + '.json').success(function(data, status) {
        var firstDate, graphData, minute, sentimentCalc, sentimentComb;
        sentimentComb = [];
        firstDate = null;
        minute = null;
        angular.forEach(data, function(val, key) {
          var combination, thisDate;
          thisDate = moment(val[1], "YYYY-MM-DD HH:mm:ss Z");
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
      }).error(function() {
        return $scope.error = "Error: Sentiment data not found";
      });
    });
    $scope.labels = _.range([start = 0], 99, [step = 1]);
    $scope.ticks = _.range([start = 0], 99, [step = 5]);
    $scope.chart = null;
  }
]);
