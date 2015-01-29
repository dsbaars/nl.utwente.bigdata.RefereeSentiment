angular.module("mbd.testCtrl", []).controller('TestCtrl', [
  '$scope', '$http', function($scope, $http) {
    var sentimentCalc;
    $scope.matches = [];
    sentimentCalc = [];
    $http.get('/data/Brazil-Croatia.json').success(function(data, status) {
      angular.forEach(data, function(val, key) {
        var combination;
        combination = {
          date: moment().format(val[1]),
          sentiment: val[4]
        };
        sentimentCalc.push(combination);
      });
      console.log(sentimentCalc);
    });
  }
]);
