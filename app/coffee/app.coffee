"use strict"

# Declare app level module which depends on views, and components
angular.module("mbd", [
    "mbd.sideBarCtrl"
    "mbd.matchCtrl"
    "ui.router"
]).config [
    "$stateProvider",
    "$urlRouterProvider"
    ($stateProvider, $urlRouterProvider) ->
        $urlRouterProvider.otherwise("/")

        $stateProvider
            .state('index', {
               url: "/",
               templateUrl: "/partials/index.html"
        })

        $stateProvider
            .state('match', {
                url: "/match/:index",
                templateUrl: "partials/match.html"
                controller: "MatchCtrl"
        })
]
