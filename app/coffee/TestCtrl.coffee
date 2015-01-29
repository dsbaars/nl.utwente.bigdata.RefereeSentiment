angular.module("mbd.testCtrl", []).controller('TestCtrl', [
    '$scope'
    '$http'
    ($scope, $http) ->
        $scope.matches = []
        sentimentCalc = []

        $http.get('/data/Brazil-Croatia.json').success((data, status) ->
            angular.forEach(data, (val,key) ->
                combination = {
                    date: moment().format(val[1]),
                    sentiment:  val[4]
                }

                sentimentCalc.push(combination)
                return
                )

            console.log(sentimentCalc)
            return
            )

        return
    ])
