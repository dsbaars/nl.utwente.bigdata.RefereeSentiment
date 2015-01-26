"use strict";
angular.module("mbd", ["mbd.sideBarCtrl", "mbd.matchCtrl", "ui.router"]).config([
  "$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.otherwise("/");
    $stateProvider.state('index', {
      url: "/",
      templateUrl: "partials/index.html"
    });
    return $stateProvider.state('match', {
      url: "/match/:index",
      templateUrl: "partials/match.html",
      controller: "MatchCtrl"
    });
  }
]);
