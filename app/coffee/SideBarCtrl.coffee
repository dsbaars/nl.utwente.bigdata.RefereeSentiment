angular.module("mbd.sideBarCtrl", []).controller('SideBarCtrl', [
    '$scope'
    '$http'
    ($scope, $http) ->
        $scope.matches = []
        $http.get('/data/worldcup-matches.json').success((data, status) ->
            $scope.matches = data
            return
            )

        return
    ])
