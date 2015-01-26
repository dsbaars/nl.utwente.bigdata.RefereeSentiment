'use strict'

module.exports = (grunt,options) ->
    configs = require('load-grunt-configs')(grunt)
    grunt.initConfig(configs)

    require('load-grunt-tasks')(grunt)

    grunt.registerTask('default', [
        'notify_hooks'
        'less'
        'coffee'
        'injector'
        'connect'
        'watch'
    ])

    grunt.registerTask('build-dist', [
        'notify_hooks'
        'useminPrepare'
        'concat:generated'
        'cssmin:generated'
        'uglify:generated'
        'usemin'
    ])
