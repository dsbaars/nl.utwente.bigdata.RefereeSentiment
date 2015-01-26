angular.module("mbd.sideBarCtrl", []).controller('SideBarCtrl', [
  '$scope', '$http', function($scope, $http) {
    $scope.matches = [];
    $http.get('data/worldcup-matches.json').success(function(data, status) {
      $scope.matches = data;
    });
  }
]);
