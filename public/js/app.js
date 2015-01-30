"use strict";
angular.module("mbd", ["mbd.sideBarCtrl", "mbd.matchCtrl", "mbd.testCtrl", "ngSanitize", "btford.markdown", "ui.router"]).config([
  "$stateProvider", "$urlRouterProvider", "$locationProvider", function($stateProvider, $urlRouterProvider, $locationProvider) {
    $urlRouterProvider.otherwise("/");
    $stateProvider.state('index', {
      url: "/",
      templateUrl: "/partials/index.html"
    });
    $stateProvider.state('match', {
      url: "/match/:index",
      templateUrl: "/partials/match.html",
      controller: "MatchCtrl"
    });
    return $stateProvider.state('test', {
      url: "/test",
      templateUrl: "/partials/test.html",
      controller: "TestCtrl"
    });
  }
]);
