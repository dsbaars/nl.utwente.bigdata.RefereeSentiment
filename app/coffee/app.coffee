"use strict"


# google.setOnLoadCallback(->
#     angular.bootstrap(document.body, ['mbd'])
#     return
# )

# Declare app level module which depends on views, and components
angular.module("mbd", [
    "mbd.sideBarCtrl"
    "mbd.matchCtrl"
    "ui.router"
]).config [
    "$stateProvider",
    "$urlRouterProvider"
    "$locationProvider"
    ($stateProvider, $urlRouterProvider, $locationProvider) ->

    #    $locationProvider.html5Mode(true)
        $urlRouterProvider.otherwise("/")

        $stateProvider
            .state('index', {
               url: "/",
               templateUrl: "/public/partials/index.html"
        })

        $stateProvider
            .state('match', {
                url: "/match/:index",
                templateUrl: "/public/partials/match.html"
                controller: "MatchCtrl"
        })
]
