module.exports =
    options:
        join: true
        bare: true
    default:
        expand: true
        flatten: true
        cwd: "app"
        src: ["**/*.coffee"]
        dest: "public/js"
        ext: ".js"
