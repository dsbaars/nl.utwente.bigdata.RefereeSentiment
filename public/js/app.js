"use strict";
angular.module("mbd", ["mbd.sideBarCtrl", "mbd.matchCtrl", "ui.router"]).config([
  "$stateProvider", "$urlRouterProvider", "$locationProvider", function($stateProvider, $urlRouterProvider, $locationProvider) {
    $urlRouterProvider.otherwise("/");
    $stateProvider.state('index', {
      url: "/",
      templateUrl: "/public/partials/index.html"
    });
    return $stateProvider.state('match', {
      url: "/match/:index",
      templateUrl: "/public/partials/match.html",
      controller: "MatchCtrl"
    });
  }
]);
