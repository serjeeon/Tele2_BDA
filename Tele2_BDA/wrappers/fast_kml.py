# todo: add mapping of colors based on column values?
# todo: add sorting capability?
# todo: add folders capability
# todo: add multigeometry with different geometries

import simplekml
from shapely import wkt
from shapely.geometry.base import dump_coords
import logging

def _process_file_name(file_name):
    if not file_name.endswith('.kml'):
        file_name += '.kml'
    return file_name

def _process_color(geometry_type, sharedstyle, color_mode, color, alpha):
    if geometry_type == 'Point':
        substyle = sharedstyle.iconstyle
    elif geometry_type == 'LineString':
        substyle = sharedstyle.linestyle
    elif geometry_type == 'Polygon':
        substyle = sharedstyle.polystyle
    else:
        logging.critical('Unknown geometry_type')
    substyle.colormode = color_mode
    if color_mode == simplekml.ColorMode.normal:
        # https://simplekml.readthedocs.io/en/latest/constants.html?#color
        substyle.color = simplekml.Color.changealphaint(alpha, color) # 0-255
    else:
        substyle.color = simplekml.Color.changealphaint(alpha, simplekml.Color.white)
    return sharedstyle

def _process_description_columns(df, wkt_column, name_column, description_columns, exclude_columns):
    if description_columns == 'all':
        description_columns = df.columns.tolist()
        if wkt_column:
            description_columns.remove(wkt_column)
        else:
            description_columns = description_columns[:-1]
        if name_column:
            description_columns.remove(name_column)
        if exclude_columns:
            description_columns = [col for col in description_columns if col not in exclude_columns]
    return description_columns
    
def _process_description(row, description_columns):
    description = None
    if description_columns:
        description = (': {}\n'.join(description_columns) + ': {}').format(*row[description_columns].values.tolist())
    logging.debug(f'desctiption: {description}')
    return description

def _process_boundaries(coords_list, altitude):
    outer_boundary = [list(t) + [altitude] for t in coords_list if isinstance(t, tuple)]
    inner_boundary = [l for l in coords_list if isinstance(l, list)]
    return (outer_boundary, inner_boundary)

def points_kml(df, file_name, wkt_column=None, name_column=None, description_columns='all', exclude_columns=None, altitude=0, label_scale=0.8, \
               color=simplekml.Color.white, alpha=255, color_mode=simplekml.ColorMode.normal, icon_href='http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png'):
    """
    Generate KML file with Points/MultiPoints layer

    Parameters:
    -----------
    df - pandas dataframe with WKT geometry;
    file_name - name of the KML file;
    wkt_column - column name of the dataframe with WKT geometry (if ommited, the last column will be taken);
    name_column - column name of the dataframe with names for the geometries (if ommited, the dataframe index will be taken);
    descrition_columns - list of column names that will be set in description balloon. If set 'all', all the columns but wkt_column and name_column will be taken;
    exclude_columns - list of column names that will be excluded from the description_columns;
    altitude - an altitude value for the geometries;
    label_scale - scale of the label;
    color - a color for the geometries (read more: https://simplekml.readthedocs.io/en/latest/constants.html?#color)
    alpha - level of opacity from 0 to 255;
    color_mode - normal/random;
    icon_href - href for the icons;
    """
    file_name = _process_file_name(file_name)
    description_columns = _process_description_columns(df, wkt_column, name_column, description_columns, exclude_columns)
    kml = simplekml.Kml()
    sharedstyle = simplekml.Style()
    sharedstyle.iconstyle.icon.href = icon_href
    sharedstyle = _process_color('Point', sharedstyle, color_mode, color, alpha)
    sharedstyle.labelstyle.scale = label_scale
    for index, row in df.iterrows():
        shape = wkt.loads(row[wkt_column]) if wkt_column else wkt.loads(row[-1])
        name = str(row[name_column]) if name_column else str(index)
        description = _process_description(row, description_columns)
        logging.debug(f'shape_type: {shape.type}')
        if shape.type == 'Point':
            outer_boundary, _ = _process_boundaries(dump_coords(shape), altitude)
            pnt = kml.newpoint(
                name=name,
                description=description,
                coords=outer_boundary,
                altitudemode = simplekml.AltitudeMode.relativetoground
            )
            pnt.extrude = 1
            pnt.style = sharedstyle
        elif shape.type == 'MultiPoint':
            multipnt = kml.newmultigeometry(
                name=name,
                description=description
            )
            for coords_list in dump_coords(shape):
                outer_boundary, _ = _process_boundaries(coords_list, altitude)
                pnt = multipnt.newpoint(
                    coords=outer_boundary,
                    altitudemode = simplekml.AltitudeMode.relativetoground
                )
                pnt.extrude = 1
            multipnt.style = sharedstyle
        else:
            print(f'{name} has bad geometry')
    kml.save(file_name)

def lines_kml(df, file_name, wkt_column=None, name_column=None, description_columns='all', exclude_columns=None, altitude=0, width=3, \
              color=simplekml.Color.red, alpha=200, color_mode=simplekml.ColorMode.normal, label_visibility=False):
    """
    Generate KML file with LineStrings/MultiLineStrings layer

    Parameters:
    -----------
    df - pandas dataframe with WKT geometry;
    file_name - name of the KML file;
    wkt_column - column name of the dataframe with WKT geometry (if ommited, the last column will be taken);
    name_column - column name of the dataframe with names for the geometries (if ommited, the dataframe index will be taken);
    descrition_columns - list of column names that will be set in description balloon. If set 'all', all the columns but wkt_column and name_column will be taken;
    exclude_columns - list of column names that will be excluded from the description_columns;
    altitude - an altitude value for the geometries;
    width - width of the lines;
    color - a color for the geometries (read more: https://simplekml.readthedocs.io/en/latest/constants.html?#color)
    alpha - level of opacity from 0 to 255;
    color_mode - normal/random;
    label_visibility - whether labels will be visible or not;
    """
    file_name = _process_file_name(file_name)
    description_columns = _process_description_columns(df, wkt_column, name_column, description_columns, exclude_columns)
    kml = simplekml.Kml()
    sharedstyle = simplekml.Style()
    sharedstyle = _process_color('LineString', sharedstyle, color_mode, color, alpha)
    sharedstyle.linestyle.width = width
    sharedstyle.linestyle.gxlabelvisibility = label_visibility
    for index, row in df.iterrows():
        shape = wkt.loads(row[wkt_column]) if wkt_column else wkt.loads(row[-1])
        name = str(row[name_column]) if name_column else str(index)
        description = _process_description(row, description_columns)
        logging.debug(f'shape_type: {shape.type}')
        if shape.type == 'LineString':
            outer_boundary, _ = _process_boundaries(dump_coords(shape), altitude)
            ls = kml.newlinestring(
                name=name,
                description=description,
                coords=outer_boundary,
                altitudemode=simplekml.AltitudeMode.relativetoground
            )
            ls.extrude = 1
            ls.style = sharedstyle
        elif shape.type == 'MultiLineString':
            multils = kml.newmultigeometry(
                name=name,
                description=description
            )
            for coords_list in dump_coords(shape):
                outer_boundary, _ = _process_boundaries(coords_list, altitude)
                ls = multils.newlinestring(
                    coords=outer_boundary,
                    altitudemode = simplekml.AltitudeMode.relativetoground
                )
                ls.extrude = 1
            multils.style = sharedstyle
        else:
            print(f'{name} has bad geometry')
    kml.save(file_name)

def polygons_kml(df, file_name, wkt_column=None, name_column=None, description_columns='all', exclude_columns=None, altitude=100, \
                 color=simplekml.Color.red, alpha=200, color_mode=simplekml.ColorMode.normal):
    """
    Generate KML file with Polygons/MultiPolygons layer

    Parameters:
    -----------
    df - pandas dataframe with WKT geometry;
    file_name - name of the KML file;
    wkt_column - column name of the dataframe with WKT geometry (if ommited, the last column will be taken);
    name_column - column name of the dataframe with names for the geometries (if ommited, the dataframe index will be taken);
    descrition_columns - list of column names that will be set in description balloon. If set 'all', all the columns but wkt_column and name_column will be taken;
    exclude_columns - list of column names that will be excluded from the description_columns;
    altitude - an altitude value for the geometries;
    color - a color for the geometries (read more: https://simplekml.readthedocs.io/en/latest/constants.html?#color)
    alpha - level of opacity from 0 to 255;
    color_mode - normal/random;
    """
    file_name = _process_file_name(file_name)
    description_columns = _process_description_columns(df, wkt_column, name_column, description_columns, exclude_columns)
    kml = simplekml.Kml()
    sharedstyle = simplekml.Style()
    sharedstyle = _process_color('Polygon', sharedstyle, color_mode, color, alpha)
    for index, row in df.iterrows():
        shape = wkt.loads(row[wkt_column]) if wkt_column else wkt.loads(row[-1])
        name = str(row[name_column]) if name_column else str(index)
        description = _process_description(row, description_columns)
        logging.debug(f'shape_type: {shape.type}')
        if shape.type == 'Polygon':
            outer_boundary, inner_boundary = _process_boundaries(dump_coords(shape), altitude)
            pol = kml.newpolygon(
                name=name,
                description=description,
                outerboundaryis=outer_boundary,
                innerboundaryis=inner_boundary,
                altitudemode=simplekml.AltitudeMode.relativetoground
            )
            pol.extrude = 1
            pol.style = sharedstyle
        elif shape.type == 'MultiPolygon':
            multipol = kml.newmultigeometry(
                name=name,
                description=description
            )
            for coords_list in dump_coords(shape):
                outer_boundary, inner_boundary = _process_boundaries(coords_list, altitude)
                pol = multipol.newpolygon(
                    outerboundaryis=outer_boundary,
                    innerboundaryis=inner_boundary,
                    altitudemode = simplekml.AltitudeMode.relativetoground
                )
                pol.extrude = 1
            multipol.style = sharedstyle
        else:
            print(f'{name} has bad geometry')
    kml.save(file_name)