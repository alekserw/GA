

const
    gapi = require("googleapis"),
    fs = require('fs'),
    sql = require("mssql"),
    profileid = '60548736',
    key = require('./ga.json'),
    scopes = 'https://www.googleapis.com/auth/analytics.readonly',
    jwt = new gapi.auth.JWT(key.client_email, null, key.private_key, scopes),
    b = true;


var Readable = require('stream').Readable,
    stream = new Readable();


var queryParams = {
    'auth': jwt,
    'ids': 'ga:' + profileid,
    'start-date': '2016-10-03',
    'end-date': '2016-10-23',
    'metrics': 'ga:sessions',
    'dimensions': 'ga:keyword,ga:campaign,ga:city,ga:date,ga:dimension5,ga:sessionCount,ga:sourceMedium',


    //'ga:keyword,ga:campaign,ga:city,ga:date,ga:dimension5,ga:sessionCount,ga:sourceMedium', 
    //'ga:date,ga:devicecategory,ga:browser,ga:dimension5,ga:screenResolution,ga:operatingSystem,ga:sessioncount', 
    'start-index': 1,
    'max-results': 15000
};


var startDate = queryParams['start-date'],
    endDate = queryParams['end-date'];


var c = 1,
    run = true,
    total = 20000,
    outFileName = startDate.slice(5) + '_' + endDate.slice(5) + '_source' + '.txt';


var outFile = fs.createWriteStream(outFileName),
    startTime = new Date(),
    queryStat = 0;


queryParams['end-date'] = queryParams['start-date'];


var queryDate = (function() {
    var count = new Date(queryParams['start-date']);
    return function(arg) {
        count.setDate(count.getDate() + 1);
        Date.prototype.yyyymmdd = function() {
            var mm = (this.getMonth() + 1).toString();
            var dd = this.getDate().toString();
            mm = !mm[1] === false ? mm : ('0' + mm);
            dd = !dd[1] === false ? dd : ('0' + dd);
            return [this.getFullYear(), mm, dd].join('-'); // padding
        };


        return count.yyyymmdd();
    };
}());


stream._read = function() {


    jwt.authorize(function(err, response) {


        if (run) {


            function readData(err, data) {


                if (err) throw err;


                var str = '';


                // Логи
                console.log('Date : ' + data.query['start-date'] + ' totalResults : ' + data.totalResults + ' Index : ' + data.query['start-index']);
                // console.log(data);

                // уставнока кол-ва строк в запросе


                if (data.query['start-index'] == 1) {
                    total = data.totalResults;
                    queryStat += total;

                };


                // парсер заголовка


                if (data.query['start-date'] === startDate && data.query['start-index'] === 1) {


                    data.columnHeaders.forEach(function(field, i, array) {


                        if (i === array.length - 1) {
                            str += field.name.slice(3);
                        } else {
                            str += field.name.slice(3) + ',';
                        };
                    });
                };


                // парсер строк


                if (data.rows) {
                    data.rows.forEach(function(row) {
                        str += '\n';
                        row.forEach(function(field, i, array) {
                            if (i === array.length - 1) {
                                str += field;
                            } else {
                                str += field + ',';
                            }
                        });
                    });
                };


                stream.push(str);
            };


            // инкрементация даты для запроса


            // console.log('queryParams ' + queryParams['start-date']);


            gapi.analytics('v3').data.ga.get(queryParams, readData);


            if ((queryParams['start-index'] > (total - queryParams['max-results'])) || (queryParams['max-results'] > total)) {


                if (endDate !== queryParams['start-date']) {
                    queryParams['start-date'] = queryDate();
                    queryParams['end-date'] = queryParams['start-date'];
                    queryParams['start-index'] = 1;


                } else {


                    //setTimeout(function(){stream.push(null)},5000);
                    run = !run;
                };


            } else {
                queryParams['start-index'] += queryParams['max-results'];
            };


            // if (c < 5) {
            //     c++;
            //     process.stdout.write('\n'+'Счетчик : ' + c + '\n');
            // } else {
            //     console.log('Выход из цикла ...');
            //     run = !run;
            // };

            // END


        } else {
            endTime = new Date()
            endTime = Math.round((endTime - startTime) / 1000 / 60);


            console.log('\n' + 'Получено строк: ' + queryStat + '\n' + 'Время выполнения процесса: ' + endTime + ' min' + '\n');
            stream.push(null);
        };
    });
};


stream.pipe(outFile);

