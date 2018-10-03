  library("plotKML")
  library("rgdal")
  library("rgeos")
  library("RODBC")
  library("XML")
  library("sp")
  library("RJDBC")
  library("xtable")
  if (!require("RColorBrewer")) {
    install.packages("RColorBrewer")
    library(RColorBrewer)
  }
  
  
  la <- function(
    obj,
    extrude = TRUE,
    tessellate = FALSE,
    outline = TRUE,
    plot.labpt = FALSE,
    z.scale = 1,
    LabelScale = get("LabelScale", envir = plotKML.opts),
    metadata = NULL,
    html.table = NULL,
    TimeSpan.begin = "",
    TimeSpan.end = "",
    colorMode = "normal",
    ...
  ){
    
    # invisible file connection
    kml.out <- get("kml.out", envir=plotKML.fileIO)
    
    # Checking the projection is geo
    prj.check <- check_projection(obj, control = TRUE)
    
    # Trying to reproject data if the check was not successful
    if (!prj.check) {  obj <- reproject(obj)  }
    
    # Parsing the call for aesthetics
    aes <- kml_aes(obj, ...)
    
    # Read the relevant aesthetics
    poly_names <- aes[["labels"]]
    #colours <- aes[["colour"]]
    colours <-as.character(obj@data$colour)
    sizes <- aes[["size"]]
    shapes <- aes[["shape"]]
    altitude <- aes[["altitude"]]  # this only works if the altitudes have not been defined in the original sp class
    altitudeMode <- aes[["altitudeMode"]]
    balloon <- aes[["balloon"]]
    
    # Parse ATTRIBUTE TABLE (for each placemark):
    if (balloon & ("data" %in% slotNames(obj))){
      html.table <- .df2htmltable(obj@data)
    }
    
    # Folder and name of the points folder
    pl1 = newXMLNode("Folder", parent=kml.out[["Document"]])
    pl2 <- newXMLNode("name", paste(class(obj)), parent = pl1)
    
    if(plot.labpt==TRUE){
      pl1b = newXMLNode("Folder", parent=kml.out[["Document"]])
      pl2b <- newXMLNode("name", "labpt", parent = pl1b)
    }
    
    # Insert metadata:
    if(!is.null(metadata)){
      md.txt <- kml_metadata(metadata, asText = TRUE)
      txt <- sprintf('<description><![CDATA[%s]]></description>', md.txt)
      parseXMLAndAdd(txt, parent=pl1)
    }
    message("Parsing to KML...")  
    
    # Prepare data for writing
    # ==============
    
    # number of polygons:
    pv <- length(obj@polygons)
    # number of Polygons:
    pvn <- lapply(lapply(obj@polygons, slot, "Polygons"), length)
    # parse coordinates:
    coords <- rep(list(NULL), pv)
    bbox<- rep(list(NULL), pv)
    hole <- rep(list(NULL), pv)
    labpt <- rep(list(NULL), pv)
    for(i.poly in 1:pv) { 
      for(i.Poly in 1:pvn[[i.poly]]){
        # get coordinates / hole definition:
        xyz <- slot(slot(obj@polygons[[i.poly]], "Polygons")[[i.Poly]], "coords")
        cxyz <- slot(slot(obj@polygons[[i.poly]], "Polygons")[[i.Poly]], "labpt")
        # if altitude is missing, add the default altitudes:
        if(ncol(xyz)==2){  xyz <- cbind(xyz, rep(altitude[i.poly], nrow(xyz)))  }
        # format coords for writing to KML [https://developers.google.com/kml/documentation/kmlreference#polygon]:
        hole[[i.poly]][[i.Poly]] <- slot(slot(obj@polygons[[i.poly]], "Polygons")[[i.Poly]], "hole")
        coords[[i.poly]][[i.Poly]] <- paste(xyz[,1], ',', xyz[,2], ',', xyz[,3], collapse='\n ', sep = "")
        bbox[[i.poly]][[i.Poly]]<-paste("<north>",max(xyz[,2]),"</north><south>",min(xyz[,2]),"</south><east>",max(xyz[,1]),"</east><west>",min(xyz[,1]),"</west>")
        
        #nsew
        labpt[[i.poly]][[i.Poly]] <- paste(cxyz[1], ',', cxyz[2], ',', altitude[i.poly], collapse='\n ', sep = "")
      }
    }
    
    # reformatted aesthetics (one "polygons" can have multiple "Polygons"):
    poly_names.l <- list(NULL)
    for(i.poly in 1:pv){ poly_names.l[[i.poly]] <- as.vector(rep(poly_names[i.poly], pvn[[i.poly]])) }
    # polygon times (if applicable)
    TimeSpan.begin.l <- list(NULL)
    TimeSpan.end.l <- list(NULL)
    when.l <- list(NULL)
    # check if time span has been defined:
    if(all(nzchar(TimeSpan.begin))&all(nzchar(TimeSpan.end))){
      if(identical(TimeSpan.begin, TimeSpan.end)){
        if(length(TimeSpan.begin)==1){ 
          when.l = rep(TimeSpan.begin, sum(unlist(pvn))) 
        } else {
          for(i.poly in 1:pv){ when.l[[i.poly]] <- as.vector(rep(TimeSpan.begin[i.poly], pvn[[i.poly]])) }
        }} else {
          for(i.poly in 1:pv){ TimeSpan.begin.l[[i.poly]] <- as.vector(rep(TimeSpan.begin[i.poly], pvn[[i.poly]])) }
          for(i.poly in 1:pv){ TimeSpan.end.l[[i.poly]] <- as.vector(rep(TimeSpan.end[i.poly], pvn[[i.poly]])) }
        }
    }          
    
    # Polygon styles
    # ==============
    if(!length(unique(colours))==1|colorMode=="normal"){
      colours.l <- list(NULL)
      for(i.poly in 1:pv){ colours.l[[i.poly]] <- as.vector(rep(colours[i.poly], pvn[[i.poly]])) }    
      txts <- sprintf('<Style id="poly%s"><LineStyle><color>%s</color></LineStyle><PolyStyle><color>%s</color><outline>%s</outline><fill>%s</fill></PolyStyle><BalloonStyle><text>$[description]</text></BalloonStyle></Style>', 1:sum(unlist(pvn)), unlist(colours.l),unlist(colours.l), rep(as.numeric(outline), sum(unlist(pvn))), as.numeric(!(unlist(hole))))
      parseXMLAndAdd(txts, parent=pl1)
    } else {
      # random colours:
      txts <- sprintf('<Style id="poly%s"><PolyStyle><colorMode>random</colorMode><outline>%s</outline><fill>%s</fill></PolyStyle><BalloonStyle><text>$[description]</text></BalloonStyle></Style>', 1:sum(unlist(pvn)), rep(as.numeric(outline), sum(unlist(pvn))), as.numeric(!(unlist(hole))))
      parseXMLAndAdd(txts, parent=pl1)
    }
    
    # Point styles
    # ==============
    if(plot.labpt == TRUE){
      sizes.l <- list(NULL)
      shapes.l <- list(NULL)
      # reformat size / shapes:
      for(i.poly in 1:pv){sizes.l[[i.poly]] <- as.vector(rep(sizes[i.poly], pvn[[i.poly]])) }
      for(i.poly in 1:pv){shapes.l[[i.poly]] <- as.vector(rep(shapes[i.poly], pvn[[i.poly]])) }    
      txtsp <- sprintf('<Style id="pnt%s"><LabelStyle><scale>%.1f</scale></LabelStyle><IconStyle><color>ffffffff</color><scale>%s</scale><Icon><href>%s</href></Icon></IconStyle><BalloonStyle><text>$[description]</text></BalloonStyle></Style>', 1:sum(unlist(pvn)), rep(LabelScale, sum(unlist(pvn))), unlist(sizes.l), unlist(shapes.l))
      parseXMLAndAdd(txtsp, parent=pl1b)
      
      # Writing labpt
      # ================  
      if(all(is.null(unlist(TimeSpan.begin.l))) & all(is.null(unlist(TimeSpan.end.l)))){
        if(all(is.null(unlist(when.l)))){
          # time span undefined:
          txtc <- sprintf('<Placemark><name>%s</name><styleUrl>#pnt%s</styleUrl><Point><extrude>%.0f</extrude><altitudeMode>%s</altitudeMode><coordinates>%s</coordinates></Point></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), rep(as.numeric(extrude), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(labpt)))
        } else {
          txtc <- sprintf('<Placemark><name>%s</name><styleUrl>#pnt%s</styleUrl><TimeStamp><when>%s</when></TimeStamp><Point><extrude>%.0f</extrude><altitudeMode>%s</altitudeMode><coordinates>%s</coordinates></Point></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), unlist(when.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(labpt)))  
        } } else{
          # fixed begin/end times:
          txtc <- sprintf('<Placemark><name>%s</name><styleUrl>#pnt%s</styleUrl><TimeSpan><begin>%s</begin><end>%s</end></TimeSpan><Point><extrude>%.0f</extrude><altitudeMode>%s</altitudeMode><coordinates>%s</coordinates></Point></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), unlist(TimeSpan.begin.l), unlist(TimeSpan.end.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(labpt)))
        }
      
      parseXMLAndAdd(txtc, parent=pl1b)
    }
    # finished writing the labels
    
    # Writing polygons
    # ================
    
    if(length(html.table)>0){   
      html.table.l <- list(NULL)
      for(i.poly in 1:pv){ html.table.l[[i.poly]] <- as.vector(rep(html.table[i.poly], pvn[[i.poly]])) }    
      
      # with attributes:
      if(all(is.null(unlist(TimeSpan.begin.l))) & all(is.null(unlist(TimeSpan.end.l)))){
        if(all(is.null(unlist(when.l)))){
          # time span undefined:
          ###РљРћР Р•Р–РРњ РўРЈРў:
          #print (sum(unlist(pvn)))
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><Region><Lod><minLodPixels>4</minLodPixels><minFadeExtent>16</minFadeExtent><maxFadeExtent>2048</maxFadeExtent></Lod><LatLonAltBox>%s<minAltitude>0</minAltitude><maxAltitude>20000</maxAltitude><altitudeMode>absolute</altitudeMode></LatLonAltBox></Region><description><![CDATA[%s]]></description><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>',
                         unlist(poly_names.l), 1:length(unlist(coords)), unlist(bbox),unlist(html.table.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))
        } else {
          
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><TimeStamp><when>%s</when></TimeStamp><description><![CDATA[%s]]></description><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>',
                         unlist(poly_names.l), 1:sum(unlist(pvn)), unlist(when.l), unlist(html.table.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))
        }} else {
          
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><description><TimeSpan><begin>%s</begin><end>%s</end></TimeSpan><![CDATA[%s]]></description><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>',
                         unlist(poly_names.l), 1:sum(unlist(pvn)), unlist(TimeSpan.begin.l), unlist(TimeSpan.end.l), unlist(html.table.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))
        }
    }
    
    # without attributes:
    else{
      if(all(is.null(unlist(TimeSpan.begin.l))) & all(is.null(unlist(TimeSpan.end.l)))){
        if(all(is.null(unlist(when.l)))){
          # time span undefined:
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))
        } else {
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><TimeStamp><when>%s</when></TimeStamp><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), unlist(when.l), rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))  
        }} else {   
          txt <- sprintf('<Placemark><name>%s</name><styleUrl>#poly%s</styleUrl><TimeSpan><begin>%s</begin><end>%s</end></TimeSpan><Polygon><extrude>%.0f</extrude><tessellate>%.0f</tessellate><altitudeMode>%s</altitudeMode><outerBoundaryIs><LinearRing><coordinates>%s</coordinates></LinearRing></outerBoundaryIs></Polygon></Placemark>', unlist(poly_names.l), 1:sum(unlist(pvn)), TimeSpan.begin, TimeSpan.end, rep(as.numeric(extrude), sum(unlist(pvn))), rep(as.numeric(tessellate), sum(unlist(pvn))), rep(altitudeMode, sum(unlist(pvn))), paste(unlist(coords)))     
        }
    }
    
    parseXMLAndAdd(txt, parent=pl1)
    
    # save results: 
    assign("kml.out", kml.out, envir=plotKML.fileIO)
    
  }
  
  ch <- odbcConnect("teradata")
  pol<-sqlQuery(ch," SELECT ne_id, count_subs_id, rate, b, sector_name, users_4g, STECHNOLOGY,
                            cast(report_month as date) as report_month, cast(report_month as date) as next_month
                    from UAT_DM.al_ne_stats_vis;",
                dec=",", buffsize=1000000, rows_at_time=10240, stringsAsFactors=F)
  
  
  ne_id_char <- paste(pol$SECTOR_NAME)
  #.ST_ASText()
  o<-sapply(seq(1:length(ne_id_char)), y=pol, FUN=function(x,y){
    #
    b<-try(readWKT(y$b[x],p4s="+proj=longlat +datum=WGS84")@polygons[[1]]);
    if(class(b) == "try-error" | is.null(b) )
    {
      #РїСѓСЃС‚РѕР№ РїРѕР»РёРіРѕРЅ
      c1 = cbind(0, 0)
      r1 = rbind(c1, c1[1, ])
      P1 = Polygons(list(Polygon(r1)), ID = as.character(x))
      b<-P1
      print(x)
    }
    else
    {
      #b@ID<-as.character(y$CELLID[x])
      b@ID<-as.character(x)
    }
    b
  })
  
  
  b<-paste(iconv(rep("Название", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(ne_id_char, from="CP1251", to="UTF-8"),
           iconv(rep("STECHNOLOGY", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(pol$STECHNOLOGY, from="CP1251", to="UTF-8"),
           iconv(rep("Месяц", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(pol$report_month, from="CP1251", to="UTF-8"),
           iconv(rep("Количество пользователей", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(pol$count_subs_id),
           iconv(rep("users_4g", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(pol$users_4g),
           iconv(rep("Доля пользователей, у которых есть 4G", nrow(pol)), from="CP1251", to="UTF-8"),
           iconv(pol$rate), sep='\n')
  
  b <- paste(	iconv(rep("<table cellpadding='5' width='400' border='1'>
                      <col width='80'>
                      <col width='80'>
                      <col width='90'>
                      <col width='80'>
                      <col width='80'>
                      <col width='80'>
                        <tr>
                        <td>Название</td>
                        <td>STECHNOLOGY</td>
                      <td>Месяц</td>
                        <td>Количество пользователей</td>
                        <td>users_4g</td>
                        <td>4G провал</td>
                        </tr>
                        <tr>
                        <td>", nrow(pol)), from="CP1251", to="UTF-8"),
              iconv(ne_id_char, from="CP1251", to="UTF-8"),
              iconv("</td>
                    <td>", from="CP1251", to="UTF-8"),
              iconv(pol$STECHNOLOGY, from="CP1251", to="UTF-8"),
              iconv("</td><td>", from="CP1251", to="UTF-8"),
              iconv(pol$report_month, from="CP1251", to="UTF-8"),
              iconv("</td>
                    <td>", from="CP1251", to="UTF-8"),
              iconv(pol$count_subs_id, from="CP1251", to="UTF-8"),
              iconv("</td>
                    <td>", from="CP1251", to="UTF-8"),
              iconv(pol$users_4g, from="CP1251", to="UTF-8"),
              iconv("</td>
                    <td>", from="CP1251", to="UTF-8"),
              iconv(paste(sprintf('%.2f', pol$rate * 100), '%', sep=''), from="CP1251", to="UTF-8"),
              iconv(rep("</td>
                        </tr>
                        </table>", nrow(pol)), from="CP1251", to="UTF-8"), sep="")
  
  col<-rep("#FFa000a0",length(ne_id_char))
  pal<-rev(10,"RdYlGn")
  cp<-pal[cut(pol$rate,breaks=quantile(pol$rate, probs=seq(0,1,by=1/10)))]
  cp<-pal[cut(pol$rate,breaks=10)]
  col<-paste("#a0",substr(cp,6,7),substr(cp,4,5),substr(cp,2,3),sep="")
  
  #pal<-rev(brewer.pal(nrow(pol),"RdYlGn"))
  #newcol <- colorRampPalette(pal)
  #cp <- newcol(nrow(pol))[as.numeric(cut(pol$rate,breaks = nrow(pol)))]
  #col<-paste("#a0",substr(cp,6,7),substr(cp,4,5),substr(cp,2,3),sep="")
  
  
  
  pol$colors <- 0
  for (m in unique(pol$report_month)) {
    df_ = pol[pol$report_month == m,]
    pal<-rev(brewer.pal(nrow(pol),"RdYlGn"))
    newcol <- colorRampPalette(pal)
    cp <- newcol(nrow(df_))[as.numeric(cut(df_$rate,breaks = nrow(df_)))]
    pol$colors[pol$report_month == m]<-paste("#a0",substr(cp,6,7),substr(cp,4,5),substr(cp,2,3),sep="")
  }
  

  
  
  altitude_const = 50 / min(pol$rate[pol$rate > 0])
  altitude_rate <- pol$rate * altitude_const / max(pol$rate)
  altitude_rate[altitude_rate < 50] <- 50
  
  # d<-data.frame(cbind(paste(pol$ne_id), c(0.75),100,"normal",paste(pol$ne_id), 100,col),stringsAsFactors = F)
  d <- data.frame(cbind(ne_id_char, c(0.1),100,"normal",ne_id_char, 100,pol$colors),stringsAsFactors = F)
  names(d)<-c("name","alpha","levels","colorModeEnum","description","flats", "colour")
  df<-SpatialPolygonsDataFrame(SpatialPolygons(o, proj4string=CRS("+proj=longlat +datum=WGS84")),d)
  #b <- xtable(pol)
  kml_open("C:/Users/andrey.lukyanenko/Desktop/data_quality_3g.kml")
  la(obj=df,file.name="C:/Users/andrey.lukyanenko/Desktop/data_quality_3g.kml",altitude=altitude_rate, labels=ne_id_char,
     points_names=ne_id_char,alpha=0.1,  colorModeEnum="normal", html.table=b , plot.labpt=F,
     TimeSpan.begin=substr(pol$report_month, 0, 7), TimeSpan.end=substr(pol$report_month, 0, 7))
  kml_close("C:/Users/andrey.lukyanenko/Desktop/data_quality_3g.kml")
  
  
