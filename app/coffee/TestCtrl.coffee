angular.module("mbd.testCtrl", []).controller('TestCtrl', [
    '$scope'
    '$http'
    ($scope, $http) ->
        $scope.matches = []
        sentimentCalc = []
        $scope.matches = []
        $scope.events = []
        $scope.lines = []

        $http.get('/data/worldcup-matches.json').success((data, status) ->
            $scope.matches = data
            $scope.match = data[0]
            $scope.home_events = data[0].home_team_events
            $scope.away_events = data[0].away_team_events

            angular.forEach($scope.home_events, (val, key) ->
                $scope.lines.push({value: Number(val.time), text: val.type_of_event, class: 'home'})
                return
            )

            angular.forEach($scope.away_events, (val, key) ->
                $scope.lines.push({value: Number(val.time), text: val.type_of_event, class: 'away'})
                return
            )
        )

        $http.get('/data/Brazil-Croatia.json').success((data, status) ->
            sentimentComb = []
            firstDate = null
            minute = null
            angular.forEach(data, (val,key) ->
                # We make sure the response is ordered by time
                thisDate = moment(val[1])

                if firstDate is null
                    firstDate = thisDate.clone()
                    minute = 0
                else
                    minute = thisDate.diff(firstDate, 'minutes')

                if val[4] <= 0
                    combination = {
                        minute: minute
                        date: moment().format(val[1]),
                        sentiment:  val[4]
                    }
                    sentimentComb.push(combination)
                return
                )

            sentimentCalc = _.transform(sentimentComb, (res, n, key) ->
                if res[n.minute]
                    res[n.minute] = res[n.minute] + Math.abs(n.sentiment)
                else
                    res[n.minute] = Math.abs(n.sentiment)
                return
            )

            graphData = _.map($scope.labels, (v) ->
                if sentimentCalc[v]
                    return sentimentCalc[v]
                0
                )

            console.log(graphData)

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
                        sentiment: graphData
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
