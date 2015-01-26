module.exports = (grunt) ->
    default:
        options:
            paths: ['bower_components']
        files:
            "public/css/main.css": ['app/**/*.less']
