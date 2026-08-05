const path = require('path');
const webpack = require('webpack');
const BrowserSyncPlugin = require('browser-sync-webpack-plugin');
const UglifyJsPlugin = require('uglifyjs-webpack-plugin');

const isProduction = process.env.NODE_ENV == 'production' ? true : false;

const webpack_config = {
    plugins: [],
    module: {
        rules: []
    },
    optimization: {}
};

webpack_config.mode = process.env.NODE_ENV;

webpack_config.entry = {
    'scroll-lock': './src/scroll-lock.js'
};

if (isProduction) {
    webpack_config.entry['scroll-lock.min'] = './src/scroll-lock.js';
}

webpack_config.output = {
    path: path.resolve(__dirname, 'dist'),
    filename: '[name].js',
    library: 'scrollLock',
    libraryTarget: 'umd',
    libraryExport: 'default',
    globalObject: 'this'
};

webpack_config.plugins.push(
    new webpack.DefinePlugin({
        'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV)
    })
);

webpack_config.module.rules.push({
    test: /\.js$/,
    exclude: [/node_modules/],
    loader: 'babel-loader',
    options: {
        presets: ['@babel/preset-env']
    }
});

if (isProduction) {
    webpack_config.optimization.minimizer = [
        new UglifyJsPlugin({
            include: /\.min\.js$/,
            cache: true,
            parallel: true,
            uglifyOptions: {
                compress: true,
                warnings: false,
                output: {
                    comments: false,
                    beautify: false
                }
            }
        })
    ];
} else {
    webpack_config.plugins.push(
        new BrowserSyncPlugin({
            host: 'localhost',
            port: 1337,
            files: ['./dist/**/*', './demos/**/*'],
            server: {
                baseDir: ['./'],
                index: '/demos/index.html'
            }
        })
    );

    webpack_config.watch = true;
    webpack_config.watchOptions = {
        aggregateTimeout: 100,
        ignored: /node_modules/
    };

    webpack_config.devtool = 'cheap-inline-module-source-map';
}

module.exports = webpack_config;
