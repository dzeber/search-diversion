#######################################################################
### 
###  Create tables with addon information by search group. 
###  
#######################################################################



## Overall addon counts and enabled proportions. 

## Overall occurrence counts and proportion. 
## Adjust for sampling. 
tot.prof = group.sizes[, sum(count)]
xac.tot = xac[, list(nprof = sum(nprof) * 100, 
                        prop = sum(nprof) / tot.prof), by = name]
setkey(xac.tot, name)

xac.en = xac[, list(enabled = .SD[enabled == TRUE, sum(nprof) * 100],
                    disabled = .SD[enabled == FALSE, sum(nprof) * 100]), 
                by = name]
setkey(xac.en, name)

## Join enabled counts with overall counts and compute proportions. 
xac.en = xac.tot[xac.en]
xac.en = xac.en[, list(name, enabled = enabled / nprof, 
                        disabled = disabled / nprof, 
                        nprof, prop)]
xac.en.old = xac.en

## Join fi proportions. 
setkey(xac.en, name)
setkey(fi.marg, name)
xac.en = fi.marg[xac.en, list(enabled, disabled, fi.prop, 
                            nfi.prop = 1 - fi.prop, nprof, prop)]
xac.en = xac.en[order(prop, decreasing = TRUE)]


#####

## Distributions of installs across groups. 

## Group names. 
gpnm = group.sizes[, group]
gpsize = function(n) { group.sizes[n][, count] }

## Occurrence counts by group. 
xac.gp = xac[, setNames(lapply(gpnm, function(nn) {
        .SD[grp == nn, sum(nprof)]
    }), gpnm),
    by = name]
setkey(xac.gp, name)

## Join with enabled counts. 
setkey(xac.en, name)
xac.gp = xac.en[xac.gp]

## Normalize group columns by group sizes. 
xac.gp[, eval(gpnm) := lapply(gpnm, function(nn) { get(nn) / gpsize(nn) })]
## Normalize rows to 1. 
xac.gp[, rs := rowSums(xac.gp[, gpnm, with = FALSE])]
xac.gp[, eval(gpnm) := lapply(gpnm, function(nn) { get(nn) / rs })]
xac.gp[, rs := NULL]
xac.gp = xac.gp[order(prop, decreasing = TRUE)][, 
        c("name", gpnm, "nprof", "prop", "enabled", "disabled"), with = FALSE]
        

#####

## Distributions of installs across groups by enabled state. 

xac.gpen = copy(xac)
## Normalize by group sizes. 
xac.gpen[, prop.gp := nprof / gpsize(grp)]
## Normalize to 1. 
xac.gpen = xac.gpen[, list(grp, prop.gp = prop.gp / sum(prop.gp)), 
                        by = list(name, enabled)]
## Rearrange. 
## If no observations for one enabled state, proportions will all be NA. 
xac.gpen = xac.gpen[, unlist(lapply(c(TRUE, FALSE), function(en) {
        v = setNames(lapply(gpnm, function(gn) {
            p = .SD[enabled == en & grp == gn, prop.gp]
            if(length(p) == 0) return(NA_real_)
            p
        }), sprintf("%s:%s", 
            c("TRUE" = "enabled", "FALSE" = "disabled")[[as.character(en)]], gpnm))
        ## Replace NA with 0 unless entire vector is NA. 
        vna = is.na(unlist(v))
        if(any(vna) && !all(vna)) v[vna] = 0
        v
    }), recursive = FALSE), by = name]
    
## Join overall columns. 
setkey(xac.gpen, name)
xac.gpen = xac.tot[xac.gpen][, c(names(xac.gpen), "nprof", "prop"), with = FALSE] 
    

## Distributions of enabled states by group. 

xac.engp = copy(xac)
## Normalize by total installed in group.
xac.engp = xac.engp[, prop.en := nprof / sum(nprof), by = list(name, grp)]

## Rearrange. 
## If no observations for one enabled state, proportions will all be NA. 
xac.engp = xac.engp[, unlist(lapply(gpnm, function(gn) {
        v = setNames(lapply(c(TRUE, FALSE), function(en) {
            .SD[grp == gn & enabled == en, prop.en]
        }), sprintf("%s:%s", gn, c("enabled", "disabled")))
        ## Replace NA with 0 unless entire vector is NA. 
        vna = is.na(unlist(v))
        if(any(vna) && !all(vna)) v[vna] = 0
        v
    }), recursive = FALSE), by = name]
    
## Join overall columns. 
setkey(xac.engp, name)
xac.engp = xac.tot[xac.engp][, c(names(xac.engp), "nprof", "prop"), with = FALSE] 


## Resort tables. 
xac.en = xac.en[order(prop, decreasing = TRUE)]
xac.gp = xac.gp[order(prop, decreasing = TRUE)]
xac.gpen = xac.gpen[order(prop, decreasing = TRUE)]
xac.engp = xac.engp[order(prop, decreasing = TRUE)]


save(xac, xac.en, xac.gp, xac.gpen, xac.engp, 
    file = file.path(local_out_folder, "addons-tables", "tables.RData"))



## Create HTML tables. 
## Available at dzeber/Rtable.
source("Rtable.R")

table.dir = file.path(local_out_folder, "addons-tables")

html.params = list(
    en = list(filename = "byenabled", title = "Proportion enabled by addon"),
    gp = list(filename = "bygroup", title = "Group distribution by addon"),
    gpen = list(filename = "groupforenabled", 
        title = "Group distribution for enabled addons"),
    gpdis = list(filename = "groupfordisabled", 
        title = "Group distribution for disabled addons"),
    engpg = list(filename = "enabledbygroupgraphical", 
        title = "Enabled state by group (graphical)"),
    engp = list(filename = "enabledbygroup", 
        title = "Enabled state by group"), 
    figpg = list(filename = "fibygroupgraphical", 
        title = "Foreign installs by group (graphical)")
)

col.headers = list(name = "Name", 
    enabled = "Proportion of installs enabled",
    enabled2 = "Enabled",
    disabled = "Proportion of installs disabled",
    disabled2 = "Disabled",
    fi.prop = "Proportion of installs foreign-installed",
    nfi.prop = "Proportion of installs not foreign-installed",
    nprof = "# profiles with addon installed", 
    prop = "Proportion of total # profiles with addon installed")

link.block = paste(c("<div id='tablelinks'>", 
    paste(c(unlist(lapply(0:2, function(i) {
        sp = unlist(lapply(html.params[2*i + 1:2], function(r) {
            sprintf("<span><a href='%s.html'>%s</a></span>", r$filename, r$title)
        })) 
        paste(c("<div>", sp[1], "<span class='sep'>&bull;</span>", sp[2], "</div>"), collapse = "")
    })), 
        sprintf("<div><span><a href='%s.html'>%s</a></span><span class='sep'>&bull;</span></div>", 
            html.params$figpg$filename, html.params$figpg$title)
    )), "</div>"), collapse = "\n")
        
link.style = "<style>
#tablelinks {
  padding: 20px 0;
  font-style: italic;
}

#tablelinks div {
  padding: 2px 0;
}

#tablelinks a, a:visited {
  color: black;
  text-decoration: none;
}

#tablelinks a:hover {
  text-decoration: underline;
}

.sep {
  padding: 0 10px;
  font-size: large;
}</style>"

link.block = paste0(link.block, "\n", link.style)
        
## Function to replace angle brackets with code to avoid strings 
## being interpreted as HTML.
replaceBrackets = function(text) {
    gsub("<", "&lt;", gsub(">", "&gt;", text, fixed = TRUE), fixed = TRUE)
}

## Group names. 
group.names = c("def|active_search" = "Active search (default)",
    "def|low_search" = "Low search (default)", 
    "def|non_search" = "No search (default)", 
    "non_def|non_search" = "No search (non-default)",
    "non_def|search" = "Active search (non-default)")
 col.headers = c(col.headers, group.names)

## Enabled state proportions table. 
dxac.en = xac.en[, setNames(lapply(names(xac.en), function(cn) {
    if(cn %in% c("enabled", "disabled", "fi.prop", "nfi.prop", "prop")) {
        round(get(cn), 5)
    } else {
        get(cn)
    }
}), names(xac.en))]
dxac.en[, enabled.bar := enabled]
dxac.en[, fi.bar := fi.prop]
dxac.en[, name := replaceBrackets(name)]

ci = setNames(lapply(names(dxac.en), function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.en))
ci$enabled.bar$label = "Proportion enabled"
ci$fi.bar$label = "Proportion foreign-installed"
ci$nprof$formatter = "count"
ci$enabled.bar$formatter = "singleprop"
ci$fi.bar$formatter = "singleprop"
# ci$prop$formatter = "singleprop"
ci$name$width = "25%"
ci$enabled.bar[["max-width"]] = "200px"
ci$fi.bar[["max-width"]] = "200px"
ci$enabled$class = "centered"
ci$disabled$class = "centered"
ci$fi.prop$class = "centered"
ci$nfi.prop$class = "centered"
ci$nprof$class = "centered"
ci$prop$class = "centered"
ci = c(ci[1:3], ci[8], ci[4:5], ci[9], ci[6:7])

table.to.HTML(dxac.en, table.dir, row.index = TRUE, 
    head = html.params$en$title, col = ci, 
    html.filename = html.params$en$filename, 
    data.filename = html.params$en$filename,
    top = link.block, open = FALSE)

## Group distributions table. 
dxac.gp = xac.gp[, setNames(lapply(seq_along(xac.gp), function(j) {
    if(j %in% c(2:6, 8:10)) {
        round(get(names(xac.gp)[j]), 5)
    } else {
        get(names(xac.gp)[j])
    }
}), names(xac.gp))]
dxac.gp[, disabled := NULL]
dxac.gp = dxac.gp[, c(1:4, 6, 5, 7:9), with = FALSE]
dxac.gp$group.bar = lapply(1:nrow(dxac.gp), function(i) { 
    as.vector(unlist(dxac.gp[i, 2:6, with = FALSE]))
})
dxac.gp[, name := replaceBrackets(name)]

ci = setNames(lapply(names(dxac.gp), function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.gp))
ci$group.bar$label = "Group distribution"
ci$nprof$formatter = "count"
ci$group.bar$formatter = "multiprop"
ci$enabled$formatter = "singleprop"
ci$name$width = "20%"
ci$enabled[["max-width"]] = "150px"
ci$enabled[["width"]] = "10%"
ci$group.bar[["max-width"]] = "200px"
ci$group.bar[["width"]] = "15%"
for(i in 2:8) {
    ci[[names(ci)[i]]]$class = "centered"
}
ci = c(ci[1:6], ci[10], ci[7:9])

table.to.HTML(dxac.gp, table.dir, row.index = TRUE, 
    head = html.params$gp$title, col = ci, 
    html.filename = html.params$gp$filename, 
    data.filename = html.params$gp$filename,
    top = link.block, open = FALSE)

    
## Group distributions by enabled state table. 
dxac.gpen = xac.gpen[, setNames(lapply(seq_along(xac.gpen), function(j) {
    if(j %in% c(2:11, 13)) {
        d = round(get(names(xac.gpen)[j]), 5)
        d[is.na(d)] = 0
        d
    } else {
        get(names(xac.gpen)[j])
    }
}), names(xac.gpen))]
dxac.gpen = dxac.gpen[, c(1:4, 6, 5, 7:9, 11, 10, 12:13), with = FALSE]
dxac.gpen$enabled.bar = lapply(1:nrow(dxac.gpen), function(i) { 
    as.vector(unlist(dxac.gpen[i, 2:6, with = FALSE]))
})
dxac.gpen$disabled.bar = lapply(1:nrow(dxac.gpen), function(i) { 
    as.vector(unlist(dxac.gpen[i, 7:11, with = FALSE]))
})
dxac.gpen[, name := replaceBrackets(name)]

nm = unlist(lapply(strsplit(names(dxac.gpen), ":", fixed = TRUE), function(r) {
    r[[length(r)]]
}))
ci = setNames(lapply(nm, function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.gpen))
ci$enabled.bar$label = "Group distribution (enabled)"
ci$disabled.bar$label = "Group distribution (disabled)"
ci$nprof$formatter = "count"
ci$enabled.bar$formatter = "multiprop"
ci$disabled.bar$formatter = "multiprop"
ci$name$width = "20%"
ci$enabled.bar[["width"]] = "200px"
ci$disabled.bar[["width"]] = "200px"
for(i in 2:13) {
    ci[[names(ci)[i]]]$class = "centered"
}
ci = c(ci[1:6], ci[14], ci[7:11], ci[15], ci[12:13])

oh = list(list(ncol = 1), 
    list(ncol = 6, label = "Group distribution of enabled installs"),
    list(ncol = 6, label = "Group distribution of disabled installs"),
    list(ncol = 3))

## Generate separate tables for enabled and disabled. 
table.to.HTML(dxac.gpen, table.dir, row.index = TRUE, 
    head = html.params$gpen$title, col = ci[-(8:12)], outer = oh[-3],
    html.filename = html.params$gpen$filename, 
    data.filename = html.params$gpen$filename,
    top = link.block, open = FALSE)

table.to.HTML(dxac.gpen, table.dir, row.index = TRUE, 
    head = html.params$gpdis$title, col = ci[c(1, 8:13, 7, 14:15)], outer = oh[-2],
    html.filename = html.params$gpdis$filename, 
    data.filename = html.params$gpdis$filename,
    top = link.block, open = FALSE)

    
## Enabled state by group table. 
dxac.engp = xac.engp[, setNames(lapply(seq_along(xac.engp), function(j) {
    if(j %in% c(2:11, 13)) {
        d = round(get(names(xac.engp)[j]), 5)
        d[is.na(d)] = 0
        d
    } else {
        get(names(xac.engp)[j])
    }
}), names(xac.engp))]
for(nn in names(dxac.engp)[seq(2, 10, by = 2)]) {
    gpnm = strsplit(nn, ":", fixed = TRUE)[[1]][1]
    dxac.engp[[sprintf("%s.bar", gpnm)]] = dxac.engp[[nn]]
}
dxac.engp[, name := replaceBrackets(name)]

nm = unlist(lapply(strsplit(names(dxac.engp), ":", fixed = TRUE), function(r) {
    if(length(r) == 2) paste0(r[2],"2") else r
}))
ci = setNames(lapply(nm[1:13], function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.engp)[1:13])
ci$nprof$formatter = "count"
ci$name$width = "25%"
for(i in 2:13) {
    ci[[names(ci)[i]]]$class = "centered"
}
ci = c(ci[1:7], ci[10:11], ci[8:9], ci[12:13])

oh = c(list(list(ncol = 1)), 
    lapply(names(group.names)[c(1:3,5,4)], function(nn) {
        list(ncol = 2, 
            label = sprintf("%s proportions", group.names[[nn]]))
    }),
    list(list(ncol = 2)))

## Generate separate tables for graphical and non. 
table.to.HTML(dxac.engp, table.dir, row.index = TRUE, 
    head = html.params$engp$title, col = ci, outer = oh,
    html.filename = html.params$engp$filename, 
    data.filename = html.params$engp$filename,
    top = link.block, open = FALSE)


nm = unlist(lapply(strsplit(names(dxac.engp), ".", fixed = TRUE), function(r) {
    r[[1]]
}))
ci = setNames(lapply(nm[-(2:11)], function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.engp)[-(2:11)])
ci$nprof$formatter = "count"
ci$name$width = "25%"
for(i in 2:3) {
    ci[[names(ci)[i]]]$class = "centered"
}
for(i in 4:8) { 
    ci[[i]]$formatter = "singleprop" 
    ci[[i]][["max-width"]] = "200px"
    ci[[i]][["width"]] = "10%"
}
ci = c(ci[1], ci[4:8], ci[2:3])
ci = c(ci[1:4], ci[6], ci[5], ci[7:8])

oh = list(list(ncol = 1), 
    list(ncol = 5, label = "Proportions of installs enabled"), 
    list(ncol = 2))

table.to.HTML(dxac.engp, table.dir, row.index = TRUE, 
    head = html.params$engpg$title, col = ci, outer = oh,
    html.filename = html.params$engpg$filename, 
    data.filename = html.params$engpg$filename,
    top = link.block, open = FALSE)

    
## Foreign-installed state by group table. 
fi.gp = fi.gp[, setNames(lapply(names(group.names), function(gn) {
        i = which(.SD[, grp == gn])
        if(length(i) == 0) 0 else .SD[i, fi.prop]
    }), names(group.names)), by = name]
fi.gp = xac.tot[fi.gp]
fi.gp = fi.gp[order(prop, decreasing = TRUE)]
    
dxac.figp = fi.gp[, setNames(lapply(seq_along(fi.gp), function(j) {
    if(j %in% 3:8) {
        d = round(get(names(fi.gp)[j]), 5)
        d[is.na(d)] = 0
        d
    } else {
        get(names(fi.gp)[j])
    }
}), names(fi.gp))]
dxac.figp[, name := replaceBrackets(name)]

ci = setNames(lapply(names(dxac.figp), function(cn) { 
    list(label = col.headers[[cn]])
}), names(dxac.figp))
ci$nprof$formatter = "count"
ci$name$width = "30%"
for(i in 2:8) {
    ci[[i]]$class = "centered"
}
for(i in 4:8) {
    ci[[i]]$formatter = "singleprop"
    ci[[i]]$width = "10%"
}
ci = c(ci[1], ci[4:8], ci[2:3])

oh = list(list(ncol = 1), 
    list(ncol = 5, label = "Proportions of installs foreign-installed"), 
    list(ncol = 2))


## Generate separate tables for graphical and non. 
table.to.HTML(dxac.figp, table.dir, row.index = TRUE, 
    head = html.params$figpg$title, col = ci, outer = oh,
    html.filename = html.params$figpg$filename, 
    data.filename = html.params$figpg$filename,
    top = link.block, open = FALSE)


    
#############################################
    
    
## Look at most common co-installed addons. 
    
## Convert to table. 

## Only keep for profiles with up to 15 addons for now. 
xaa = xaa[unlist(lapply(xaa, function(r) { r[[1]]$nao <= 15 }))]
xaa = xaa[order(unlist(lapply(xaa, function(r) { r[[1]]$nao })))]

## Layer list first by # addons then by search group. 
nao = unlist(lapply(xaa, function(r) { r[[1]]$nao }))
xang = lapply(1:15, function(i) {
    xn = xaa[nao == i]
    gp = unlist(lapply(xn, function(r) { r[[1]]$grp }))
    xn = setNames(lapply(xn, "[[", 2), gp)
    ## Restructure inner lists. 
    xn = lapply(xn, function(r) {
        ## For each search group. 
        r = lapply(r, function(rr) {
            ## Order alphabetically by name. 
            ord = order(unlist(lapply(rr$aod, "[[", "name")))
            aod = rr$aod[ord]
            ## For each of the 50 combinations
            list(name = unlist(lapply(aod, "[[", "name")),
                fi = unlist(lapply(aod, "[[", "fi")),
                enabled = unlist(lapply(aod, "[[", "enabled")),
                count = rr$count * 100)
        })
        rd = data.table(count = unlist(lapply(r, "[[", "count")))
        for(nn in c("name", "fi", "enabled")) {
            rd[[nn]] = lapply(r, "[[", nn)
        }
        rd
    })
    xn[names(group.names)]
})


## Create table from here. 

## Display the list of names on separate lines in each cell. 
formatNames = function(nn) {
    ## Escape any angle brackets to avoid problems with HTML. 
    nn = gsub("<", "&lt;", gsub(">", "&gt;", nn, fixed = TRUE), fixed = TRUE)
    ## Join names into a string with line breaks. 
    paste(nn, collapse = "<br>")
}

## Represent boolean values visually as dot. 
## If all values are the same for entire cell, use a single value. 
formatBooleans = function(bb) {
    # ubb = unique(bb)
    ## Use single value if all the same.
    # if(length(ubb) == 1) bb = ubb
    bb = paste(ifelse(bb, "&#9679;", ""), collapse = "<br>")
}

table.dir = file.path(local_out_folder, "addons-tables")

col.headers = list(name = "Name", 
    fi = "Foreign-installed", 
    enabled = "Enabled",
    count = "# profiles")

## Group names. 
group.names = c("def|active_search" = "Active search (default)",
    "def|low_search" = "Low search (default)", 
    "def|non_search" = "No search (default)", 
    "non_def|search" = "Active search (non-default)",
    "non_def|non_search" = "No search (non-default)") 
 
 ## Column info template. 
cci = list(name = list(class = "namecol"),
    fi = list(class = "centered indicator_dot"),
    enabled = list(class = "centered indicator_dot"),
    count = list(class = "centered", formatter = "count"))
## Add labels. 
for(nn in names(cci)) {
    cci[[nn]]$label = col.headers[[nn]]
}

## Styling for boolean indicator. 
extra.html = "<style>
    .indicator_dot { color: #006666; }
    .namecol { 
        white-space: nowrap; 
        padding-left: 10px; 
    }
    tbody > tr td:nth-child(4n+5), thead > tr:last-child th:nth-child(4n+5) {
      border-right: 1px solid #999;
      padding-right: 10px;
    }
    thead > tr:last-child th:nth-last-child(4n) {
      border-left: 1px solid #999;
    }
    tr th:first-child, tr td:first-child {
      padding-right: 10px;
    }
    thead > tr:last-child th:nth-child(2) {
      border-left: none;
    }
    td { padding: 10px 0; }
    #tableselect { 
      padding: 20px 0;
      font-style: italic;
      font-size: 15px;
    }
    #naddons {
      margin: 0 6px;
    }
    </style>"
    
## Selector to navigate to other tables. 
selector = "
    <div id='tableselect'>
        Switch to profiles with 
        <select id='naddons'>
            %s
        </select>
        addons      
    </div>
    <script>
        $('#naddons').change(function() {
            window.location.href = $(this).val();
        });
    </script>"
selector = sprintf(selector, 
    paste(sprintf("<option value='top%s.html'%s>%s</option>", 1:15, "%s", 1:15), 
        collapse = "\n"))

extra.html = paste(extra.html, selector, sep = "\n")
 

for(i in seq_along(xang)) {
    dd = xang[[i]]
    
    ## Create properly formatted table. 
    dd = lapply(dd, function(d) {
        d = copy(d)
        d[, name := lapply(name, formatNames)]
        d[, fi := lapply(fi, formatBooleans)]
        d[, enabled := lapply(enabled, formatBooleans)]
    })
    
    ## Create column info structure. 
    ci = unlist(lapply(dd, function(gg) { cci }), recursive = FALSE)
    
    ## Create outer headers. 
    oh = lapply(names(dd), function(gn) {
        list(ncol = 4, label = group.names[[gn]])
    })
        
    ## Cbind tables. 
    dd = as.data.table(unlist(dd, recursive = FALSE))
    
    selected = rep("", 15)
    selected[i] = "selected"
  
    table.to.HTML(dd, table.dir, row.index = TRUE, 
        head = sprintf(
            "Top collections of co-installed addons for profiles with %s addon%s", 
            i, if(i == 1) "" else "s"),
        col = ci, 
        outer = oh, 
        html.filename = sprintf("top%s", i), 
        data.filename = sprintf("top%s", i),
        top = do.call(sprintf, as.list(c(extra.html, selected))),
        open = FALSE)
}


    