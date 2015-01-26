angular.module("mbd.matchCtrl", ['chart.js']).controller('MatchCtrl', [
    '$scope'
    '$http'
    '$stateParams'
    ($scope, $http, $stateParams) ->
        $scope.matches = []
        $scope.events = []
        $http.get('data/worldcup-matches.json').success((data, status) ->
            $scope.matches = data
            $scope.match = data[$stateParams.index]
            $scope.home_events = data[$stateParams.index].home_team_events
            $scope.away_events = data[$stateParams.index].away_team_events

            return
            )

        $scope.labels = _.range([start=0], 100, [step=5])

        $scope.series = [
            "Sentiment"
        ]
        $scope.data = [
            [
                5
                3
                2
                1
                5
                2
                6
                9
                5
                3
                1
                3
                5
                4
                5
                4
                6
                4
                3
                5
            ]
        ]
        $scope.onClick = (points, evt) ->
            console.log points, evt
            return

        return
    ])