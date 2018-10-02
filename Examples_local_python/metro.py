"""Getting and processing metro data."""
import requests
import json
import pandas as pd
import os
import configparser
import turbodbc
import editdistance


class MetroData(object):
    """
    A class used to work with Moscow metro data.

    The main purpose of this class is to get Moscow metro data from yandex api and hh api. We need both because
    they have different important data. Of course you can also load already processed data.
    Then you can get data with some statistics from teradata and merge it. Or load from local file.
    And you can also visualize this info with bokeh.
    There are many hardcoded things, but there is no other way.

    Methods
    -------
    _get_data_from_api()
        Gets data from yandex and hh api
    _save_metro_data()
        Processes metro data, separates it into stations, lines and connections and saves
    load_teradata_data(use_sql=False, load_local=False, dsn='', sql='', file_name='',
                           settings_file_name='settings.ini', sep='\t')
        Loads data from teradata or local file
    combine_files(result_file_name='new_stations.csv', loaded_file_name='', return_result=False)
        Combines previously processed data and data from teradata/local file.

    Example:
    --------
    >>>metro_data = MetroData(no_process=True)
    >>>metro_data.combine_files('new_stations.csv')

    """

    def __init__(self, path='', stations_file_name='stations.csv', lines_file_name='lines.csv',
                 connections_file_name='connections.csv',
                 yandex_api='https://metro.yandex.ru/api/get-scheme-metadata?id=1&lang=ru',
                 hh_api='https://api.hh.ru/metro/1', no_process=True):
        """
        Parameters:
        -----------
        *_file_name : str
            File names to save or load data
        *api : str
            Links to relevant api
        no_process : bool
            True for loading data from files, False for downloading and processing data
        """

        self.path = path
        self.stations_file_name = os.path.join(self.path, stations_file_name)
        self.lines_file_name = os.path.join(self.path, lines_file_name)
        self.connections_file_name = os.path.join(self.path, connections_file_name)
        self.settings_file_name = None
        self.data = None
        self.dsn = None
        self.new_stations = None
        self.loaded_files = []
        self.combined_files = []

        if no_process:
            if not os.path.exists(self.stations_file_name):
                raise FileNotFoundError(f'File {self.stations_file_name} not found!')
            if not os.path.exists(self.lines_file_name):
                raise FileNotFoundError(f'File {self.lines_file_name} not found!')
            if not os.path.exists(self.connections_file_name):
                raise FileNotFoundError(f'File {self.connections_file_name} not found!')

            print('Loading files')
            self.lines = pd.read_csv(self.lines_file_name)
            self.stations = pd.read_csv(self.stations_file_name)
            self.connections = pd.read_csv(self.connections_file_name)
            print('Done')

        else:
            print('Getting data from api.')
            self.yandex_api = yandex_api
            self.hh_api = hh_api
            self._get_data_from_api()

            print('Processing and saving data.')
            self._save_metro_data()

    def _get_data_from_api(self):
        """Get data from api."""
        try:
            r = requests.get(self.yandex_api)
        except OSError:
            print('Refresh Forefront VPN!')

        self.metro = json.loads(json.loads(r.text)['data'])
        r = requests.get(self.hh_api)
        self.stations_geo = json.loads(r.text)

    def _save_metro_data(self):
        """
        Process downloaded data.

        Data is separated into three groups:
        - lines: line id, name and color;
        - connections: all pairs of connected stations and their line;
        - stations: station id, name, coordinates and line;
        """

        # process metro lines
        lines = pd.DataFrame(self.metro['lines']).transpose().reset_index()
        lines = lines[['index', 'name', 'color']]
        lines.columns = ['line', 'name', 'colour']
        # in fact I don't use "stripe" anywhere in the code, but it could be used to define stripes of lines in vis
        lines['stripe'] = None
        lines['line'] = lines['line'].astype(int)
        lines.to_csv(self.lines_file_name, index=False)
        self.lines = lines

        # process stations data
        stations = pd.DataFrame(self.metro['stations']).transpose().reset_index()
        stations = stations[['index', 'name', 'lineId']]

        """
        The following code takes geo data (lat and lon) from hh api and combines it with data from yandex api.
        
        There are stations with the same name on different lines, so they need to be matched by station and and line.
        Lines have different numeration is two apis, so we need to match them manually.
        Station names also can be different, so I create dictionary for local renaming.
        Station "улица сергея эйзенштейна" is only in hh api, so don't use it.
        
        """
        for l in self.stations_geo['lines']:
            # match line id
            if l['id'] == '95':
                line_id = 14
            elif l['id'] == '96':
                line_id = 13
            elif l['id'] == '97':
                line_id = 15
            else:
                line_id = int(l['id'])

            for s in l['stations']:
                # match name
                name = s['name'].lower().strip()
                # TODO maybe also use fuzzy matching here
                rename_dict = {'библиотека им.ленина': 'библиотека имени ленина',
                               'воробьевы горы': 'воробьёвы горы',
                               'щелковская': 'щёлковская',
                               'семеновская': 'семёновская',
                               'молодежная': 'молодёжная',
                               'филевский парк': 'филёвский парк',
                               'новые черемушки': 'новые черёмушки',
                               'теплый стан': 'тёплый стан',
                               'савеловская': 'савёловская',
                               'тропарево': 'тропарёво',
                               'хорошево': 'хорошёво',
                               'хорошевская': 'хорошёвская'}

                if name in rename_dict.keys():
                    name = rename_dict[name]

                if name == 'улица сергея эйзенштейна':
                    pass
                else:
                    stations.loc[(stations['lineId'] == line_id) & (stations['name'].str.lower() == name),
                                 'longitude'] = s['lng']
                    stations.loc[(stations['lineId'] == line_id) & (stations['name'].str.lower() == name),
                                 'latitude'] = s['lat']

        # take only necessary columns and rename them
        stations = stations[['index', 'latitude', 'longitude', 'name', 'lineId']]
        stations.columns = ['id', 'latitude', 'longitude', 'name', 'line']
        stations['line'] = stations['line'].astype(int)

        stations.to_csv(self.stations_file_name, index=False)
        self.stations = stations

        # process connections
        connections = pd.DataFrame(self.metro['links']).transpose()[['fromStationId', 'toStationId']]
        connections.columns = ['station1', 'station2']
        connections['station1'] = connections['station1'].astype(int)
        stations['id'] = stations['id'].astype(int)

        # getting line
        connections = pd.merge(connections, stations,
                               left_on='station1', right_on='id')[['station1', 'station2', 'line']]

        connections.to_csv(self.connections_file_name, index=False)
        self.connections = connections
        print('Done')

    def _set_teradata_params(self):
        """Helper function to set parameters and options."""

        self.config = configparser.ConfigParser()
        self.config.read(self.settings_file_name, encoding='utf-8')
        for key, value in self.config['TERADATA'].items():
            setattr(self, key, value)

    def load_teradata_data(self, use_sql=False, load_local=False, dsn='', sql='', file_name='',
                           settings_file_name='settings.ini', sep='\t'):
        """
        Get data from teradata.

        You can import a table from teradata or load a file which was already exported.
        Important: if you want to load data for several months, it would be better to do it for each month separately.
        When you merge it with the original data, there will be missing values (we don't have clients on all stations)
        and it will be difficult to decide what month should be in the row.

        Parameters:
        -----------
        use_sql - load data from Teradata table
        sql - sql query
        dsn - pass dsn value or parameters will be loaded from settings file
        load_local - load local file
        file_name - local file name
        """
        if (use_sql | load_local) is False:
            raise ValueError('Define one of load parameters!')

        if (load_local & use_sql) is True:
            raise ValueError("Can't use both load options!")

        if use_sql:
            # set dsn
            self.settings_file_name = os.path.join(self.path, settings_file_name)
            if dsn == '':
                if not os.path.exists(self.settings_file_name):
                    raise FileNotFoundError(f'File {self.settings_file_name} not found!')
                self._set_teradata_params()
            else:
                self.dsn = dsn

            try:
                connection = turbodbc.connect(dsn=self.dsn)
            except Exception as e:
                print('Wrong teradata dsn!')
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(e)
            data = pd.DataFrame(cursor.fetchallnumpy())
            cursor.close()
            connection.close()
            if data.shape[0] == 0:
                raise ValueError('SQL query returned empty result!')

        else:
            assert os.path.exists(os.path.join(self.path, file_name)), 'File not found!'
            data = pd.read_csv(os.path.join(self.path, file_name), sep=sep)

        if len(self.loaded_files) == 0:
            attr_name = 'data'
        else:
            attr_name = 'data' + str(len(self.loaded_files))
        setattr(self, attr_name, data)
        self.loaded_files.append(attr_name)
        print('Data loaded.')

    def combine_files(self, result_file_name='new_stations.csv', loaded_file_name='', return_result=False):
        """
        Combines files processed earlier and teratada file.

        There will be a lot of hardcoded processing. Endure it, or simply use this method and get the result.

        I suppose that the data from teradata has ne_id address and
        some columns with data (maybe even without them).

        And it is important that I suppose than all metro stations are in Moscow.

        Parameters:
        ----------
        result_file_name : str
            File name for saving.
        return_result : bool
            Whether to return the processed file

        :return:
        Retuns the processed file.
        """

        if loaded_file_name == '':
            data = self.__getattribute__(self.loaded_files[-1])
        else:
            if loaded_file_name not in self.loaded_files:
                raise ValueError('No such file name!')
            data = self.__getattribute__(loaded_file_name)

        if 'address' not in data.columns:
            raise ValueError('Please, use the supposed column name (address)!')

        """
        Merging previously processed files and a newly loaded file by station name and line id.
        
        There is a line "Москва Город\Москва Город\Станция метро Фрунзенская", which differs from other values patters.
        So fixing it.

        Other columns have pattern: Москва Город\Станция метро Фрунзенская

        So a part before "\" is useless.

        In some rows line name is in parenthesis, sometimes it is separated by comma, or by single "
        or there could be no line name at all. Need to process all these cases.
        """
        data.loc[data['address'].str.contains('нзенская'),
                 'address'] = 'Москва Город\Станция метро Фрунзенская'

        data['station_name'] = data['address'].apply(lambda x: x.split('\\')[1])

        # Getting basic line names
        data['line_name'] = ''
        for i, l in enumerate(data['station_name'].values):
            if '(' in l:
                data.iloc[i, -1] = l.split('(')[1].strip(')')
            elif ',' in l:
                data.iloc[i, -1] = l.split(',')[1].strip(')')
            elif '"' in l:
                data.iloc[i, -1] = l.split('"')[-1].strip(')')
            else:
                pass

        # getting stations names
        data['station_name'] = data['station_name'].apply(lambda x: x if ',' not in x else x.split(',')[0])

        # getting rid of line names and useless words
        bad_strings = ['\(.*\)', 'Станция метро', 'станция метро', 'Станция', 'Ст. метро', 'метро', '"',
                       'Люблинско-Дмитровской линии',
                       'Таганско-Краснопресненской линии', 'Сокольнической линии', 'Кольцевой линии', 'МЦК']
        for s in bad_strings:
            data['station_name'] = data['station_name'].str.replace(s, '')

        data['station_name'] = data['station_name'].str.strip()

        # Not all station names match exactly in datasets, so need to make a dictionary
        # TODO maybe also use fuzzy matching here
        replace_dict = {i: j for i, j in zip(['Кузнецкий мост', 'Охотный ряд', 'Лермонтовский Проспект',
                                              'улица Старокачаловская', 'улица академика Янгеля',
                                              'Новые Черемушки', 'Хорошевская', 'Тропарево', 'Тёплый стан',
                                              'Октябрьское поле', 'Речной Вокзал', 'Проспект мира',
                                              'Деловой Центр'],
                                             ['Кузнецкий Мост', 'Охотный Ряд', 'Лермонтовский проспект',
                                              'Улица Старокачаловская', 'Улица Академика Янгеля',
                                              'Новые Черёмушки', 'Хорошёвская', 'Тропарёво', 'Тёплый Стан',
                                              'Октябрьское Поле', 'Речной вокзал',
                                              'Проспект Мира', 'Деловой центр'])}

        data['station_name'] = data['station_name'].apply(
            lambda x: x if x not in replace_dict.keys() else replace_dict[x])

        """
        Now I need to match lines in the previously processed file and in the file from Teradata.
        It is done in several steps.

        1. Fuzzy matching - it can find matches for most values.
        2. Now we need to find line names for other stations. We can use the previous file.
        But there is a little problem: there are stations, which exist on two or more lines, for which one line name is 
        defined and others aren't. Need to process it.

        """

        # building dictionary of matches

        my_lines = sorted(self.lines['name'].unique())
        teratata_lines = sorted(data['line_name'].unique())

        try:
            import editdistance
        except ModuleNotFoundError:
            print('Please install "editdistance" library!')

        lines_match = []
        lines_dist = []
        # finding most similar line names
        for l1 in teratata_lines:
            distances = [(l2, editdistance.eval(l2, l1)) for l2 in my_lines]
            best_match, dist = min(distances, key=lambda x: x[1])
            lines_match.extend([[i[0] for i in distances if i[1] == dist]])
            lines_dist.extend([dist])

        # getting most similar line names. Threshold was determined manually
        lines_replace = {i[0]: i[1][0] if i[2] < 8 else '' for i in zip(teratata_lines, lines_match, lines_dist)}

        # it is easier to add these two matches manually
        lines_replace[' ТПК'] = 'Большая кольцевая линия'
        lines_replace['Третий пересадочный контур'] = 'Большая кольцевая линия'

        # replace line names
        data['line_name'] = data['line_name'].apply(lambda x: x if x not in lines_replace.keys() else lines_replace[x])

        # dicts of line id to line name and vice versa
        line_to_id = {i: j for i, j in zip(self.lines['name'], self.lines['line'])}
        id_to_line = {v: k for k, v in line_to_id.items()}

        # filling in empty lines
        for i, row in data.iterrows():
            if row['line_name'] == '':
                st_name = row['station_name']
                # data for this station in our df and old one
                df_s = data.loc[data['station_name'] == st_name]
                stations_s = self.stations.loc[self.stations['name'] == row['station_name']]

                # if there are multiple lines for this station, find the correct one
                if len(df_s) > 1:
                    t_line_s = [i for i in df_s.line_name.values if i != '']
                    m_line_s = list(stations_s.line.values)
                    # correct line is the one which isn't already filled in
                    impute_line = list(set([id_to_line[i] for i in m_line_s]) - set(t_line_s))[0]

                elif len(df_s) == 1:
                    line_id = stations_s['line'].values[0]
                    impute_line = self.lines.loc[self.lines['line'] == line_id, 'name'].values[0]

                data.loc[(data['station_name'] == row['station_name']) & (
                            data['line_name'] == ''), 'line_name'] = impute_line

        data['line'] = data['line_name'].apply(lambda x: x if x not in line_to_id.keys() else line_to_id[x])

        # merging data at last
        new_stations = pd.merge(self.stations, data, left_on=['name', 'line'],
                                     right_on=['station_name', 'line'], how='left')

        new_stations.to_csv(result_file_name, index=False)

        if len(self.combined_files) == 0:
            attr_name = 'new_stations'
        else:
            attr_name = 'new_stations' + str(len(self.combined_files))

        setattr(self, attr_name, new_stations)
        self.combined_files.append(attr_name)
        print('Done')
        if return_result:
            return new_stations