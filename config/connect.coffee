rewriteModule = require('http-rewrite-middleware')

module.exports =
    development:
        options:
            port: 9000
            hostname: "*"
            base: "public"
            livereload: true
            middleware: (connect, options) ->
                middlewares = []

                middlewares.push connect().use( '/bower_components', connect.static('./bower_components') )

                middlewares.push rewriteModule.getMiddleware([
                    from: "(^((?!javascripts|stylesheets|img|fonts|css|js|data|partials|static$).)*$)"
                    to: "/index.html#$1"
                    ])

                options.base = [options.base]  unless Array.isArray(options.base)

                directory = options.directory or options.base[options.base.length - 1]
                options.base.forEach (base) ->
                    middlewares.push connect.static(base)
                    return

                connect().use(
                    '/bower_components',
                    connect.static('./bower_components')
                )

                return middlewares
