module.exports = (grunt) ->
    default:
        options:
            paths: ['public/bower_components']
        files:
            "public/css/main.css": ['app/**/*.less']
