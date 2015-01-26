module.exports = (grunt) ->
    less:
        files: ['app/**/*.less']
        tasks: ['less']
        options:
            livereload: true
    coffee:
        files: ['app/**/*.coffee']
        tasks: ['coffee']
        options:
            livereload: true
