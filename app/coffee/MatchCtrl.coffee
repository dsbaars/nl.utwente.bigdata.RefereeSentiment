angular.module("mbd.matchCtrl", ['chart.js']).controller('MatchCtrl', [
    '$scope'
    '$http'
    '$stateParams'
    ($scope, $http, $stateParams) ->
        $scope.matches = []
        $scope.events = []
        $scope.lines = []
        $http.get('data/worldcup-matches.json').success((data, status) ->
            $scope.matches = data
            $scope.match = data[$stateParams.index]
            $scope.home_events = data[$stateParams.index].home_team_events
            $scope.away_events = data[$stateParams.index].away_team_events

            angular.forEach($scope.home_events, (val, key) ->
                $scope.lines.push({value: Number(val.time), text: val.type_of_event, class: 'home'})
                return
            )

            angular.forEach($scope.away_events, (val, key) ->
                $scope.lines.push({value: Number(val.time), text: val.type_of_event, class: 'away'})
                return
            )

            jsonData = [
                18,
                24,
                96,
                824,
                840,
                792,
                424,
                128,
                552,
                595,
                357,
                392,
                224,
                112,
                189,
                56,
                70,
                0,
                119,
                84,
                91,
                105,
                98,
                21,
                91,
                0,
                0,
                0,
                35,
                112,
                222,
                24,
                0,
                24,
                0,
                40,
                160,
                96,
                184,
                168,
                196,
                63,
                21,
                42,
                70,
                105,
                28,
                70,
                21,
                119,
                49,
                70,
                14,
                42,
                0,
                56,
                161,
                7,
                63,
                28,
                21,
                133,
                7,
                0,
                0,
                0,
                0,
                0,
                12,
                108,
                72,
                42,
                12,
                24,
                6,
                36,
                110,
                30,
                30,
                0,
                85,
                175,
                35,
                150,
                105,
                85,
                230,
                1105,
                1809,
                1048,
                676,
                424,
                396,
                412,
                248
                ]

            c3.generate(
                tooltip:
                    show: false
                point:
                    show: false
                bindto: "#matchChart"
                legend:
                    show: false
                data:
                    type: 'spline'
                    x: "x"
                    json:
                        x: $scope.labels
                        sentiment: jsonData
                axis:
                    x:
                        tick:
                            values: $scope.ticks
                grid:
                    x:
                        lines: $scope.lines
            )
            return
            )

        $scope.labels = _.range([start = 0], 99, [step = 1])
        $scope.ticks = _.range([start = 0], 99, [step = 5])

        $scope.chart = null

        return
    ])
