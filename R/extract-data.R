#######################################################################
### 
###  Extracting and munging FHR data to investigate 
###  problems around search diversion. 
###  
#######################################################################


library(data.table)

## Using RHIPE to launch MR jobs. 

source("/usr/local/share/load-fhr-query.R")

out_folder = "/user/dzeber/fhr/search-div"
local_out_folder = "fhr/search-div"

out.data = file.path(out_folder, "data")


#######################################################################


## Utility functions. 

## Input vector of strings that are supposed to be dates. 
## Output NULL means error occurred. 
## Use valid-dates instead?
# check.dates = function(datestr) {
    # tryCatch(as.Date(datestr), error = function(e) { NULL })
# }

## Input data.days list and current version. 
## Output dates mapping to version updates, or NULL if no version info. 
get.versions = function(dd, curr.ver) {
    dd = dd[order(names(dd))]
    vers = unlist(lapply(dd, function(s) {
        vu = s[["org.mozilla.appInfo.versions"]]
        if(is.null(vu) || length(vu) == 0)
            return(NULL)
        ## Handle different field versions. 
        if(vu[["_v"]] == 2) vu$appVersion else vu$version
    }))
    
    ## Some profiles have no version fields. 
    if(length(vers) == 0) {
        ## Get current version instead. 
        rhcounter("COND", sprintf("no versions; curr = %s", curr.ver), 1)
        
        ## If don't even have current version, skip record. 
        if(is.na(curr.ver)) return(NULL)
        ## Otherwise record version for current day. 
        vers = character()
        vers[max(names(dd))] = curr.ver
    }
    
    ## Sanitize version list. 
    vers = substr(vers, 1, 2)
    dv = c(1, diff(as.numeric(vers)))
    vers = vers[which(dv != 0)]
    ## At this point, should have length(vers) >= 1.
    
    # ## Note profiles still on v21
    # if(vers[length(vers)] == "21")
        # rhcounter("COND", "v21", 1)
    ## Check for NA versions.
    if(any(is.na(vers))) {
        rhcounter("COND", "NA vers", 1)
        ## Remove NAs
        vers = vers[!is.na(vers)]
    }
    vers
}


## Input data.days list. 
## Output list mapping dates (with non-zero search volume)
## to data frames of search counts by provider & SAP.
## If no searches in profile, outputs NULL. 
## Does not check for but requires package data.table. 
get.search.counts.df = function(days) {
    sc = tryCatch({
    lapply(days, function(d) { 
        ## For each day:
        ## Pre-process search counts. 
        s = d$org.mozilla.searches.counts
        ## Check for length 0 as well. 
        if(is.null(s) || length(s) == 0) return(NULL)
        
        if(!is.list(s)) s = as.list(s)
        s[["_v"]] = NULL
        ## Recheck for length 0, in case "_v" was only field. 
        if(length(s) == 0) return(NULL)
        
        ## Convert any NULL search counts to NA. 
        s[unlist(lapply(s, is.null))] = NA
        
        ## Remove rows with NA or non-positive count. 
        ss = as.numeric(unlist(s))
        to.remove = is.na(ss) | ss <= 0
        n.to.remove = sum(to.remove)
        if(n.to.remove == length(s)) return(NULL)
        if(n.to.remove > 0) s = s[!to.remove]
        
        ## At this point there is at least one valid search count for this day.
        
        sn = names(s)
        
        ## Resolve SPV/SPA.
        tryCatch({
            sn = strsplit(sn, ".", fixed = TRUE)
            ## Output is list of length 2 character vectors. 
            sn = lapply(sn, function(nv) {
                ## Missing/problem name.
                if(is.null(nv) || is.na(nv)) 
                    c(NA, NA)
                else {
                    ## No "." in name - assume string is SAP.
                    if(length(nv) == 1)
                        c(NA, nv)
                    else
                        ## Just in case a spv has a dot in the name: 
                        c(paste(nv[-length(nv)], collapse = "."), nv[length(nv)])
                }
            })
            
            ## Format as data frame. 
            data.frame(spv = sapply(sn, "[[", 1), 
                sap = sapply(sn, "[[", 2),
                ct = as.numeric(unlist(s)), 
                stringsAsFactors = FALSE)
        }, error = function(e) { 
            rhcounter("_ERR_", "bad_sc_day", 1)
            return(NULL) 
        })
    })
    },error = function(e) { 
        rhcounter("_ERR_", "bad_sc_prof", 1)
        return(NULL)
    })
    
    ## Remove any null entries in days list. 
    no.sc = unlist(lapply(sc, is.null))
    no.sc.n = sum(no.sc)
    if(no.sc.n == length(no.sc)) return(NULL)    
    if(no.sc.n > 0) return(sc[!no.sc])
    sc
}


## Tag days with reference field values (that are not necessarily recorded on the same days. 
## Input list named by dates and dated list flags named by field name. 
tag.days = function(dd, ..., before = c("NA", "first"), enlist = NULL) {
    flags = list(...)
    ## First order dates in increasing order. 
    flags = lapply(flags, function(f) { f[order(names(f))] })
    before = match.arg(before)
    setNames(lapply(names(dd), function(dn) {
        d = dd[[dn]]
        if(is.character(enlist)) 
            d = setNames(list(d), enlist)
        
        for(fln in names(flags)) {
            fl = flags[[fln]]
            fn = names(fl) <= dn
            d[[fln]] = if(any(fn)) {
                fl[[max(which(fn))]]
            } else { 
                if(before == "first")
                    fl[[1]]
                else
                    NA
            }
        }
        d
    }), names(dd))
}

## Search partner shortnames. 
## Read from https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/healthreport/dataformat.html#org-mozilla-searches-counts.
partners = c(
    "amazon-co-uk",
    "amazon-de",
    "amazon-en-GB",
    "amazon-france",
    "amazon-it",
    "amazon-jp",
    "amazondotcn",
    "amazondotcom",
    "amazondotcom-de",
    "aol-en-GB",
    "aol-web-search",
    "bing",
    "eBay",
    "eBay-de",
    "eBay-en-GB",
    "eBay-es",
    "eBay-fi",
    "eBay-france",
    "eBay-hu",
    "eBay-in",
    "eBay-it",
    "google",
    "google-jp",
    "google-ku",
    "google-maps-zh-TW",
    "mailru",
    "mercadolibre-ar",
    "mercadolibre-cl",
    "mercadolibre-mx",
    "seznam-cz",
    "twitter",
    "twitter-de",
    "twitter-ja",
    "yahoo",
    "yahoo-NO",
    "yahoo-answer-zh-TW",
    "yahoo-ar",
    "yahoo-bid-zh-TW",
    "yahoo-br",
    "yahoo-ch",
    "yahoo-cl",
    "yahoo-de",
    "yahoo-en-GB",
    "yahoo-es",
    "yahoo-fi",
    "yahoo-france",
    "yahoo-fy-NL",
    "yahoo-id",
    "yahoo-in",
    "yahoo-it",
    "yahoo-jp",
    "yahoo-jp-auctions",
    "yahoo-mx",
    "yahoo-sv-SE",
    "yahoo-zh-TW",
    "yandex",
    "yandex-ru",
    "yandex-slovari",
    "yandex-tr",
    "yandex.by",
    "yandex.ru-be"
)

## Classify search provider name as:
## { starts with "google", ie. google-* } -> "google"
## otherwise { in partner list } -> "partner"
## otherwise { starts with "other", ie. other-* } -> "other"
## otherwise "misc".
spv.levels = c("google", "partner", "other", "misc")

spv.type = {
    f = function(spv) {
        ## Google searches. 
        gg = grepl("^google", spv, ignore.case = TRUE)
        ## Partner searches
        pp = !gg & spv %in% partners
        ## "other"
        oo = !gg & !pp & grepl("^other", spv, ignore.case = TRUE)
        spv[gg] = "google"
        spv[pp] = "partner"
        spv[oo] = "other"
        spv[!gg & !pp & !oo] = "misc"
        spv
    }
    fe = new.env(parent = parent.env(environment(f)))
    assign("partners", partners, envir = fe)
    environment(f) = fe
    f
}
  
## Compute profile age on given day in days.
## dn is date string and pcd is Date object. 
prof.age = function(dn, pcd) {
    tryCatch(as.numeric(as.Date(dn) - pcd), error = function(e) { NA })
}


## Convert MR output to data table. 
## Combine key and value into rows using optional valnames for values. 
## Then rbind rows into a data.table.
make.dt = function(x, valnames = NULL) {
    require(data.table)
    cf = function(r) { c(as.list(r[[1]]), setNames(as.list(r[[2]]), valnames)) }
    x = lapply(x, cf)
    rbindlist(x)
}
  


#######################################################################


## First collect dataset to work with.

## release/win
cond = fhr.cond.default(function(r) {
        identical(get.val(gai, "updateChannel"), "release") && 
            identical(get.val(gai, "os"), "WINNT")
    }, channel = FALSE, os = FALSE)

logic = function(k,r) { 
    dd = r$data$days
    if(length(dd) == 0) return("no_days")
    
    if(is.null(check.dates(names(dd)))) return("bad_dates")
    
    ## Restrict to later than April 2013 - for data cleanliness
    dd = dd[names(dd) >= "2013-04-01"]
    if(length(dd) == 0) return("no_recent_days")
    
    ## Outputted data. 
    res = list()
    
    ##### 
    
    ## Versions: 
    
    vers = get.versions(dd, get.val(r$geckoAppInfo, "version"))
    
    ## Check for current version matching last recorded version. 
    ## If not same, make note but continue anyway. 
    if(!identical(substr(get.val(r$geckoAppInfo, "version"), 1, 2), vers[[length(vers)]]))
        rhcounter("COND", "curr ver different", 1)
    
    ## Vector of major version numbers, 
    ## named by date on which version change was recorded. 
    res[["ver"]] = vers
    
    #####
    
    ## Daily flags. 
    
    flags = list("def" = c("org.mozilla.appInfo.appinfo", "isDefaultBrowser"),
        "blp" = c("org.mozilla.appInfo.appinfo", "isBlocklistEnabled"), 
        "uch" = c("org.mozilla.appInfo.update", "enabled"),
        "udl" = c("org.mozilla.appInfo.update", "autoDownload"))
        
    ## Separate lists of flag values, 
    ## named by date on which value was recorded. 
    fvals = lapply(flags, function(nn) {
        v = unlist(lapply(dd, function(d) { d[[nn[1]]][[nn[2]]] }))
        setNames(as.logical(v), names(v))
    })
    ## Note and remove completely missing fields. 
    fvals.no = sapply(fvals, length) == 0
    if(any(fvals.no)) {
        for(nn in names(fvals)[fvals.no])
            rhcounter("COND", sprintf("no %s", nn), 1)
        fvals = fvals[!fvals.no]
    }
    
    ## If all values are same, keep only common value, otherwise keep entire vector.
    for(nn in names(fvals)) {
        uv = unique(fvals[[nn]])
        if(length(uv) == 1)
            res[[sprintf("%s.all", nn)]] = uv
        else res[[nn]] = fvals[[nn]]
    }
    
    #####
    
    ## Search counts. 
    
    sc = get.search.counts(dd)
    if(!is.null(sc))
        res[["sc"]] = sc
    else
        rhcounter("COND", "no sc", 1)
        
    
    #####
    
    ## Browser activity. 
    
    ## Activity metrics - active days, sessions, active ticks. 
    ## List of clean/aborted active ticks & firstPaint, 
    ## named by day on which session started. 
    ## Keep entries for each active day in profile (empty list if no session info).
    adays = lapply(dd, function(s) {
        tms = lapply(c(clt = "cleanActiveTicks", 
            abt = "abortedActiveTicks", fp = "firstPaint"), function(nn) { 
                s$org.mozilla.appSessions.previous[[nn]]
        })
        tms[!sapply(tms, is.null)]
    })
    if(all(sapply(adays, length) == 0))
        rhcounter("COND", "no act", 1)
    res[["adays"]] = adays
    
    #####
    
    ## Profile-level info. 
    
    res[["geo"]] = get.val(r, "geoCountry")
    if(is.na(res$geo)) rhcounter("COND", "NA geo", 1)
    res[["loc"]] = get.val(r$data$last$org.mozilla.appInfo.appinfo, "locale")
    if(is.na(res$loc)) rhcounter("COND", "NA loc", 1)
    
    ## Profile creation date, stored as Date object. 
    res[["pcd"]] = tryCatch(as.Date(get.val(r$data$last$org.mozilla.profile.age, "profileCreation"), origin = "1970-01-01"), 
            error = function(e) { NA })
    if(is.na(res$pcd)) rhcounter("COND", "NA prof creation date", 1)
    
    #####
    
    ## Addon info. 
    
    ext = list()
    plg.old = list()
    ## Other addons. 
    aoo = list()
    
    ## Old addon field. 
    ao1 = r$data$last$org.mozilla.addons.active
    if(!is.null(ao1)) { 
        ao1[["_v"]] = NULL
        types = sapply(ao1, "[[", "type")
        ext = ao1[types == "extension"]
        plg.old = ao1[types == "plugin"]
        aoo = ao1[types != "extension" & types != "plugin"]
    }
    
    ## New addon fields. 
    ao2 = r$data$last$org.mozilla.addons.addons
    if(!is.null(ao2)) {
        ao2v = ao2[["_v"]]
        ao2[["_v"]] = NULL
        ## Discard description, if present.
        if(ao2v == 2)
            ao2 = lapply(ao2, function(a) { a$description = NULL; a })
        types = sapply(ao2, "[[", "type")
        ext = c(ext, ao2[types == "extension"])
        plg.old = c(plg.old, ao2[types == "plugin"])
        aoo = c(aoo, ao2[types != "extension" & types != "plugin"])
    }
    ## Handle plugins, if present. 
    aop = r$data$last$org.mozilla.addons.plugins
    if(!is.null(aop)) {
        aop[["_v"]] = NULL
        aop = lapply(aop, function(a) { a$description = NULL; a })
        ## Strip names as they have no new information. 
        names(aop) = NULL
        res[["plg.new"]] = aop
    }
    if(length(ext) > 0)
        res[["ext"]] = ext
    else
        rhcounter("COND", "no extensions", 1)
    
    if(length(plg.old) > 0)
        res[["plg.old"]] = plg.old
    
    if(max(length(plg.old), length(aop)) == 0)
        rhcounter("COND", "no plugins", 1)
    
    if(length(aoo) > 0)
        res[["aoo"]] = aoo
    else 
        rhcounter("COND", "no other addons", 1)
    
    
    ## List res has (possibly missing) fields: 
    ## ver = {versions by date}, 
    ## def = {isDefault by date} OR def.all = {common isDefault value}, 
    ## blp = {isBlocklistEnabled by date} OR blp.all,
    ## uch = {is automatic update checking enabled by date} OR uch.all
    ## udl = {is automatic update downloading enabled by date} OR udl.all, 
    ## sc = {data frames of search counts (spv, sap, ct) by date}, 
    ## adays = {activity by date as list(clt, abt, fp)}, 
    ## ext = {list of extensions}, 
    ## plg.old = {list of plugins (old format)},
    ## plg.new = {list of plugins (new format)},
    ## aoo = {list of other addons}, 
    ## geo = {country string}, 
    ## loc = {locale string}, 
    ## pcd = {profile creation date as Date object}.
    
    rhcollect(k, res)
}

## Run on 1% FHR sample. 
## Function fhr.query lives in dzeber/work-tools/blob/master/R/fhr/fhr-query.R
## Already sourced earlier. 

z = fhr.query(out.data, logic = logic, conditions = cond, 
    params = list(get.versions = get.versions,
                get.search.counts = get.search.counts.df))

## Store counts in case needed later. 
ct.flds = c("mapred", "stats", "end.state")
cts = lapply(ct.flds, function(n) { z[[n]] })
names(cts) = ct.flds
cts[["cond"]] = z[[1]]$counters$COND
save(cts, file = file.path(local_out_folder, "zcts.RData"))


#######################################################################


## Look at activity and search rates. 


## By profile: activity, default status, search counts, # providers, # SAPs. 
out.actsc = file.path(out_folder, "act-sc")

m = function(k,r) {
    kk = list()
    kk$def = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    kk$ad = length(r$adays)
    kk$as = length(unlist(lapply(r$adays, "[[", "fp")))
    kk$at = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))
    kk$sc = sum(unlist(lapply(r$sc, "[", , "ct")))
    kk$nsp = length(unique(unlist(lapply(r$sc, "[", , "spv"))))
    kk$nsap = length(unique(unlist(lapply(r$sc, "[", , "sap"))))
    rhcollect(kk, 1)
}

zz = rhwatch(map = m, reduce = summer, input = out.data, 
    output = out.actsc, debug = "count", read = FALSE)


## Sample from output of above. 
zas = rhwatch(map = function(k,r) {
        nkeep = sum(runif(r) < 0.01)
        if(nkeep == 0) return()
        rhcollect(k, nkeep)
    }, reduce = summer, input = out.actsc, debug = "count", read = FALSE)

    
## Output 36321 records. 
xa = rhread(zas)
xa = make.dt(xa, "count")
save(xa, file = file.path(local_out_folder, "datasets", "act.RData"))
    


#####


## Use quantile binning to find bivariate quantiles for search counts vs. active ticks.
## Restricted to browsers set to default. 

## Compute 200 quantiles of active ticks.
 
## Get frequency table of active ticks values. 
zqat = rhwatch(map = function(k,r) {
        if(!identical(k$def, TRUE)) return()
        rhcollect(k$at, r)
    }, reduce = summer, input = out.actsc, debug = "count", read = FALSE)

xat = rhread(zqat)
## 200 quantiles of active ticks. 
qat = wtd.quantile(sapply(xat, "[[", 1), weights = sapply(xat, "[[", 2), 
    probs = 0:200/200)

## Compute quantiles of search counts based on quantile bins of active ticks.
## Treat bins as left-open/right-closed. 
## Bin label is upper endpoint. 
 
## Get frequency counts of search count values, binned by active ticks quantiles. 
zqsc = rhwatch(map = function(k,r) {
        if(!k$def) return()
        
        ## Identify active ticks bin.
        atb = qv[min(which(k$at <= qv))]
        
        rhcollect(list(atb = atb, sc = k$sc), r)
    }, reduce = summer, input = out.actsc, 
    param = list(qv = sort(unique(qat))), 
    debug = "count", read = FALSE)

xsc = rhread(zqsc)
xsc = make.dt(xsc, "count")
setkey(xsc, atb, sc)

xscq = xsc[, list(scq = {
        ## Compute 200 quantiles for each active ticks bin. 
        scq = if(sum(count) <= 200) { sc } else {
            wtd.quantile(sc, weights = count, probs = 0:200/200)
        }
        rep(scq, sum(qat == atb))
    }), by = atb]
    
xq = xscq
setnames(xq, c("at", "sc"))
save(xq, file = file.path(local_out_folder, "atscq.RData"))

###
    
## Compute cutoffs after excluding non-searchers. 
    
## 40th %ile of active ticks.  
qat40 = xq[sc >= 3, quantile(at, 0.4)[[1]]]

## 30th %iles of search counts by activity.
qsc30 = xq[sc >= 3, list(scq = quantile(sc, 0.3)), by = at]

save(qat40, qsc30, file = file.path(local_out_folder, "q-cutoffs.RData"))


#######################################################################


## Group profiles by search group, and collect stats. 


search.grps = c(def.no = "def|non_search", 
                def.low = "def|low_search",
                def.norm = "def|active_search",
                ndef.no = "non_def|non_search",
                ndef.norm = "non_def|search")
                
## Identify search group for profile based on default status, total searches 
## and total active ticks. 

group.prof = {
    gf = function(defst, sctot, attot) { 
        if(!identical(defst, TRUE)) {
            ## Non-default browser. 
            if(sctot <= 2)
                return(s.gps[["ndef.no"]])
            else
                return(s.gps[["ndef.norm"]])
        }
        
        ## Browser is default.
        if(sctot <= 2)
            return(s.gps[["def.no"]])
        
        ## Search count >= 3. 
        ## Check activity against cutoff. 
        if(attot < atq) 
            return(s.gps[["def.norm"]])
        
        ## Activity is above cutoff. 
        ## Check searches for low-search. 
        
        ## Identify interval (left-closed). 
        iat = findInterval(attot, scq$at, 
                rightmost.closed = TRUE, all.inside = TRUE)
        ## Convert to right-closed.
        if(attot > scq$at[iat])
            iat = iat + 1
            
        if(sctot <= scq$scq[iat]) {
            s.gps[["def.low"]]
        } else {
            s.gps[["def.norm"]]
        }
    }

    environment(gf) = list2env(list(s.gps = search.grps,
                                            atq = qat40,
                                            scq = qsc30))
    gf
}
                  

## Investigate different search behaviours by profile.

m = function(k,r) {
    ## Default status
    defst = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    ## Total search counts
    sctot = sum(unlist(lapply(r$sc, "[", , "ct")))
    ## Total active ticks. 
    attot = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))
    
    ## Identify search group
    sgrp = get.sgp(defst, sctot, attot)
    
    rhcounter("SEARCH_GROUP", sgrp, 1)
    
    #####
    
    ## Profile-level factors
    
    if(!is.null(r$ver)) {
        ## Latest version in profile
        max.ver = r$ver[max(names(r$ver))]
        
        ## Most common version in profile
        vers = tag(r$adays, ver = r$ver)
        vers = table(unlist(lapply(vers, "[[", "ver")), useNA = "ifany")
        common.ver = names(vers)[which.max(vers)]
    } else {
        max.ver = common.ver = NA
    }
    
    ## Profile age: 
    ## number of months from profile creation date until first active date.
    ## Truncate at after 3 years.
    prof.age = if(is.null(r$pcd)) { NA } else {
            pa = trunc(get.age(min(names(r$adays)), r$pcd) / 30.5)
            if(!is.na(pa)) {
                if(pa < 0) pa = -1
                if(pa > 36) pa = 37
            }
            pa
        }
    
    ## Join together version, profile age and country. 
    rhcollect(list(table = "info",  grp = sgrp,
                    mver = max.ver, 
                    cver = common.ver,
                    age = prof.age,
                    geo = r$geo), 1)
    
    #####
    
    ## Startup times.
    
    ## Median firstPaint time. 
    fp = unlist(lapply(r$adays, "[[", "fp"))
    ## Remove negatives. 
    mfp = if(is.null(fp)) { NA } else { median(fp[fp > 0]) }
    
    rhcollect(list(table = "startup", grp = sgrp,
                    mfp = mfp), 1)
                
    #####
    
    ## Activity
    
    ## Number of days
    ndays = length(r$adays)
    
    ## Number of sessions
    nsess = length(fp)
    
    rhcollect(list(table = "activitycounts", grp = sgrp,
                    nd = ndays,
                    ns = nsess,
                    tat = attot), 1)
    
    ## Mean number of sessions per day. 
    sess.day = round(mean(unlist(lapply(r$adays, function(s) { 
        length(s$fp) 
    }))), 2)
    
    ## Median number of (clean) active ticks per session.
    at.sess = median(unlist(lapply(r$adays, "[[", "clt")))
    
    rhcollect(list(table = "activityrates", grp = sgrp,
                    sesspd = sess.day,
                    atps = at.sess), 1)
                
    #####
    
    ## Addons
    
    if(!is.null(r$ext)) {
        ## IDs of installed extensions with enabled status
        ext.enb = lapply(r$ext, function(e) {
            !any(e$userDisabled, e$appDisabled)
        })
        
        for(en in names(ext.enb)) {
            rhcollect(list(table = "extnames", grp = sgrp,
                            name = en,
                            enabled = ext.enb[[en]]), 1)
        }
        
        ## Number of extensions installed
        n.ext = length(r$ext)

        ## Number of foreign installed
        n.fi = sum(unlist(lapply(r$ext, "[[", "foreignInstall")))
    } else {
        n.ext = n.fi = 0
    }
                    
    rhcollect(list(table = "extcount", grp = sgrp,
                    ne = n.ext,
                    nfi = n.fi), 1)
                
    #####
    
    ## Searches
    
    if(!is.null(r$sc)) {
        sc = r$sc[order(names(r$sc))]
        spv = unlist(lapply(sc, "[", , "spv"))
        
        ## Proportions of provider type.
        spv.type = round(table(get.spv(spv))/length(spv), 2)
        
        for(pn in names(spv.type)) {
            rhcollect(list(table = "spvtype", grp = sgrp,
                            spvt = pn,
                            prop = spv.type[[pn]]), 1)
        }
        
        ## Number of switches between positive sc and 0 sc across days.
        sct = lapply(sc, function(ss) { sum(ss$ct) })
        other.days = !(names(r$adays) %in% names(sc))
        if(any(other.days)) sct[names(r$adays)[other.days]] = 0
        sct = unlist(sct[order(names(sct))])
        sct = sct > 0
        
        if(length(sct) > 1) {
            n.sw.sc = sum(diff(sct) != 0)
            
            ## Proportion of days with positive searches. 
            p.pos.sc = round(mean(sct), 2)
            
            ## If more than 10 active days, record the proportion of switches.
            ## Otherwise record the number of switches. 
            vals = if(ndays > 10) {
                    list(vtype = "p", swsc = round(n.sw.sc / (length(sct) - 1), 2))
                } else {
                    list(vtype = "n", swsc = n.sw.sc)
                }
                
            rhcollect(c(list(table = "positivesc", grp = sgrp,
                                ppsc = p.pos.sc)
                                , vals), 1)
        }
        
        ## Number of search providers used
        n.spv = length(unique(spv))
        
        ## Number of switches between providers. 
        n.swp = if(n.spv == 0) { NA } else {
            sum(spv[1:(length(spv)-1)] != spv[2:length(spv)])
            ## If more than 10 switches, ignore value. 
            # if(n.swp > 10)
                # n.swp = 11
        }
            
        ## Number of SAPs used. 
        n.sap = length(unique(unlist(lapply(sc, "[", , "sap"))))
        
    } else {
        n.spv = n.swp = n.sap = NA
    }
    
    rhcollect(list(table = "spvcount", grp = sgrp,
                    nspv = n.spv,
                    nswp = n.swp,
                    nsap = n.sap), 1)
}

## Output: 

## { table = "info", grp, mver, cver, age, geo } -> count   
## { table = "startup", grp, mfp } -> count
## { table = "activitycounts", grp, nd, ns, tat } -> count
## { table = "activityrates", grp, sesspd, atps } -> count
## { table = "extcount", grp, ne, nfi } -> count
## { table = "extnames", grp, name, enabled } -> count
## { table = "spvcount", grp, nspv, nswp, nsap } -> count
## { table = "spvtype", grp, spvt, prop } -> count
## { table = "positivesc", grp, ppsc, vtype, swsc } -> count

out.grpstats = file.path(out_folder, "group-stats")

param = list(get.sgp = group.prof,
                tag = tag.days,
                get.age = prof.age,
                get.spv = spv.type)

    
z = rhwatch(map = m, reduce = summer, 
    input = out.data, output = out.grpstats,
    param = param, jobname = "Search grouping stats",
    debug = "count", read = FALSE)
    
group.sizes = z[[1]]$counters$SEARCH_GROUP


## Compute table sizes: 

zt = rhwatch(map = function(k,r) {
        rhcounter("TABLE", k$table, 1)
    }, input = out.grpstats, read = FALSE)

table.sizes = zt[[1]]$counters$TABLE

save(table.sizes, group.sizes, 
    file = file.path(local_out_folder, "sgroups.RData"))


## Overall search counts by group. 

m = function(k,r) {
    ## Default status
    defst = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    ## Total search counts
    sctot = sum(unlist(lapply(r$sc, "[", , "ct")))
    ## Total active ticks. 
    attot = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))
    
    ## Identify search group
    sgrp = get.sgp(defst, sctot, attot)
    
    rhcounter("SEARCH_VOL", sgrp, sctot)
}

param = list(get.sgp = group.prof)
   
z = rhwatch(map = m, input = out.data, param = param, 
    debug = "count", read = FALSE)
 
svol = z[[1]]$counters$SEARCH_VOL

svol = data.table(group = rownames(svol), sc = svol)
setnames(svol, 2, "sc")
setkey(svol, "group")

svol = svol[group.sizes]

save(svol, file = file.path(local_out_folder, "searches.RData"))


#######################


## Look at extension names. 

## How many names per group?
z = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "extnames")) return()
        nulls = unlist(lapply(k, is.null))
        if(any(nulls)) {
            for(nn in names(k)[nulls]) rhcounter("NULLS", nn, 1)
            k[nulls] = NA
        }
        rhcounter("GROUP", sprintf("%s (enabled is %s)", k$grp, k$enabled), 1)
    }, reduce = NULL, input = out.grpstats, read = FALSE)

ancts = z[[1]]$counters$GROUP

## Addon counts by group.  
load("/usr/local/share/addon-dict.RData")

## Function to resolve name from addon ID. 
## Aggregate addon table over ID/name combinations. 
anames = addons[, list(count = sum(count)), by = list(id, name)]
## Replace missing names with IDs. 
anames[is.na(name) | name == "", name := id]
## Key by ID for lookup. 
setkey(anames, id)

addon.namefromid = function(aid) {
    a = anames[aid]
    if(nrow(a) == 1) {
        ## If ID was not found, will give 1-row DT with name == NA. 
        ## Identify addon using ID instead. 
        if(a[, is.na(name)]) return(aid)
        ## Otherwise return name. 
        return(a[, name])
    }
    ## Otherwise use most common name. 
    ## Sometimes addon has different localized names associated with same ID. 
    a[which.max(count), name]
}
environment(addon.namefromid) = {
    e = new.env(parent = baseenv())
    e$anames = anames
    e
}

## Counts of unique addon names per group. 
## First convert the ID to name and count occurrences. 
zac = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "extnames")) return()
        
        if(is.na(k$name)) rhcounter("COND", "NA_id", 1)
        if(nchar(k$name) == 0) rhcounter("COND", "nochar_id", 1)
        
        name = addon.name(k$name)
        
        ## Shouldn't have any NAs or zero-character strings. 
        if(is.na(name)) rhcounter("COND", "NA_name", 1)
        if(nchar(name) == 0) rhcounter("COND", "nochar_name", 1)
        
        rhcollect(list(grp = k$grp, name = name), r) 
    }, 
    reduce = summer, input = out.grpstats, 
    param = list(addon.name = addon.namefromid), 
    setup = expression(map = { suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)

## Find top 1000 addons for each group. 
zbg = rhwatch(
    map = function(k,r) { 
        rhcollect(k$grp, data.table(name = k$name, count = r))
    }, 
    reduce = expression(pre = { topm = NULL },
        reduce = {
            topm = rbindlist(c(topm, reduce.values))
        }, 
        post = {
            if(nrow(topm) > 1000) 
                topm = topm[order(count, decreasing = TRUE)[1:1000]]
            rhcollect(reduce.key, topm)
            # rhcollect(reduce.key, rbindlist(topm))
        }), 
    combiner = TRUE,
    input = zac, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    debug = "count", read = FALSE)

abg = rhread(zbg)
abg = setNames(lapply(abg, "[[", 2), unlist(lapply(abg, "[[", 1)))

## Overall addon counts among top group addons. 
acts = rbindlist(lapply(names(abg), function(nn) { abg[[nn]][, group := nn] }))
setkey(acts, name)
## Compute overall observed occurrence rate, weighted by group size. 
acts = acts[, list(prop = sum(count) / sum(group.sizes[group][,count])), 
                by = name]

## Out of these, keep the top 1000. 
acts = acts[order(prop, decreasing = TRUE)[1:1000]]
setkey(acts, name)

## acts is the master list of addon names to collect.
save(acts, abg, 
    file = file.path(local_out_folder, "addons-tables", "addons-list.RData"))

## Make function to check whether to keep addon. 
keep.addon = function(aname) {
    lookup = addons.list[aname]
    ## Addon is absent if lookup has NA value for nprof. 
    lookup[, !is.na(prop)]
}
environment(keep.addon) = {
    e = new.env(parent = baseenv())
    e$addons.list = acts
    e
}

## Now collect enabled/disabled counts by group for addons from master list. 
zac = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "extnames")) return()
        
        name = addon.name(k$name)
        if(!keep.addon(name)) return()
        
        rhcollect(list(grp = k$grp, name = name, enabled = k$enabled), r) 
    }, 
    reduce = summer, input = out.grpstats, 
    param = list(addon.name = addon.namefromid, 
               keep.addon = keep.addon), 
    setup = expression(map = { suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)

xac = make.dt(rhread(zac), "nprof")

save(xac, acts, 
    file = file.path(local_out_folder, "addons-tables", "addon-counts-bygrp.RData"))



## Foreign-installed:


m = function(k,r) { 
    ## Default status
    defst = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    ## Total search counts
    sctot = sum(unlist(lapply(r$sc, "[", , "ct")))
    ## Total active ticks. 
    attot = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))

    ## Identify search group
    sgrp = get.sgp(defst, sctot, attot)

    
    ## Addons
    ao = r$ext
    
    if(!is.null(ao)) {
        ## Collect foreign-installed status, enabled status, name. 
        # n.ao = length(ao)
        
        for(nn in names(ao)) {
            ao.name = addon.name(nn)
            if(!keep.addon(ao.name)) next
            
            aod = ao[[nn]]
            fi = isTRUE(!is.null(aod$foreignInstall) && as.logical(aod$foreignInstall))
            enb = !isTRUE(any(unlist(lapply(c("appDisabled", "userDisabled"), 
                function(dn) { !is.null(aod[[dn]]) && as.logical(aod[[dn]]) 
            }))))
            
            rhcollect(list(grp = sgrp, 
                # k = n.ao, 
                name = ao.name,
                fi = fi, enb = enb), 1)
        }
    } else {
        rhcounter("COND", "no_addons", 1)
    }
    
}
   
out.grpao = file.path(out_folder, "group-addons")

param = list(get.sgp = group.prof, 
            addon.name = addon.namefromid, 
            keep.addon = keep.addon)


z = rhwatch(map = m, 
    reduce = summer, 
    input = out.data, 
    output = out.grpao,
    param = param, 
    setup = expression(map = { suppressPackageStartupMessages(library(data.table)) }), 
    debug = "count", read = FALSE)

xa = make.dt(rhread(z), "nprof")

save(xa, file = file.path(local_out_folder, 
                                "addons-tables", "addon-fi-bygrp.RData"))


## Proportions of foreign-installs by group. 
## Create new table. 
setkey(xa, grp, name)
fi.gp = xa[, list(fi = .SD[fi == TRUE, sum(nprof) * 100], 
                    nprof = sum(nprof) * 100), 
                by = list(grp, name)]
fi.gp[, fi.prop := fi / nprof]                                
              
## Overall proportions of foreign-installs
fi.marg = fi.gp[, list(fi = sum(fi), nprof = sum(nprof)), by = name]
fi.marg[, fi.prop := fi / nprof]              

fi.en = xa[, list(enabled.fi = .SD[fi == TRUE & enb == TRUE, sum(nprof) * 100], 
                enabled.nfi = .SD[fi == FALSE & enb == TRUE, sum(nprof) * 100], 
                disabled.fi = .SD[fi == TRUE & enb == FALSE, sum(nprof) * 100], 
                disabled.nfi = .SD[fi == FALSE & enb == FALSE, sum(nprof) * 100], 
                nprof = sum(nprof) * 100), 
            by = list(grp, name)]
fi.en[, `:=`(enabled.fi.prop = enabled.fi / (enabled.fi + enabled.nfi), 
            disabled.fi.prop = disabled.fi / (disabled.fi + disabled.nfi),
            fi.enabled.prop = enabled.fi / (enabled.fi + disabled.fi),
            nfi.enabled.prop = enabled.nfi / (enabled.nfi + disabled.nfi))] 




#########################################3


## Generate HTML tables. 

## Source is in ./addon-tables.R. 



######################################

            
## Look at most common co-installed addons. 


m = function(k,r) { 
    ## Default status
    defst = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    ## Total search counts
    sctot = sum(unlist(lapply(r$sc, "[", , "ct")))
    ## Total active ticks. 
    attot = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))

    ## Identify search group
    sgrp = get.sgp(defst, sctot, attot)

    
    ## Addons
    ao = r$ext
    
    if(!is.null(ao)) {
        ## Collect total number of installed addons, foreign-installed status, enabled status, and names. 
        
        aod = lapply(names(ao), function(nn) {
            ao.name = addon.name(nn)
            a = ao[[nn]]
            fi = isTRUE(!is.null(a$foreignInstall) && as.logical(a$foreignInstall))
            enb = !isTRUE(any(unlist(lapply(c("appDisabled", "userDisabled"), 
                function(dn) { !is.null(a[[dn]]) && as.logical(a[[dn]]) 
            }))))
            list(name = ao.name, fi = fi, enabled = enb)
        })
        
        rhcollect(list(grp = sgrp, aod = aod), 1)
            
    } else {
        rhcounter("COND", "no_addons", 1)
    }
    
}
   
out.grpaoco = file.path(out_folder, "group-addons-coinst")

param = list(get.sgp = group.prof, 
            addon.name = addon.namefromid)


z = rhwatch(map = m, 
    reduce = summer, 
    input = out.data, 
    param = param, 
    setup = expression(map = { suppressPackageStartupMessages(library(data.table)) }), 
    debug = "count", read = FALSE)

    
## Make sure addon lists are ordered to avoid duplicates due to permutation. 
zz = rhwatch(map = function(k,r) {
        nms = unlist(lapply(k$aod, "[[", "name"))
        k$aod = k$aod[order(nms)]
        rhcollect(k,r)
    }, 
    reduce = summer, 
    input = z, 
    output = out.grpaoco, 
    debug = "count", read = FALSE)

    
## Keep top few. 

red = expression(pre = { topm = NULL },
    reduce = {
        topm = c(topm, reduce.values)
    }, 
    post = {
        ## Different structure when going through combiner or reducer.
        ## At reduce stage, will have extra list layer. 
        ## Workaround: check for counts and unlist if necessary. 
        if(length(topm) > 0 && is.null(names(topm[[1]]))) {
            topm = unlist(topm, recursive = FALSE)
        }
        if(length(topm) > 50) {
            cts = unlist(lapply(topm, "[[", "count"))
            topm = topm[order(cts, decreasing = TRUE)[1:50]]
        }
        rhcollect(reduce.key, topm)
    }) 
        
za = rhwatch(map = function(k,r) {
        rhcollect(list(grp = k$grp, nao = length(k$aod)), 
            list(aod = k$aod, count = r))
    }, 
    reduce = red, 
    combiner = TRUE,
    input = zz,
    debug = "count", read = FALSE)

xaa = rhread(za)
save(xaa, file = file.path(local_out_folder, "addons-tables", "topbynao.RData"))



#######################################################


## { table = "extcount", grp, ne, nfi } -> count

## Look at extension counts. 

## Create table of addon install and foreign-install counts.
zec = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "extcount")) return()
        # rhcounter("GPCT", k$grp, 1)
        rhcollect(k$grp, list(n = k$ne, n.fi = k$nfi, count = r))
    }, 
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = out.grpstats, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)
    
xec = rhread(zec)
xec = setNames(lapply(xec, "[[", 2), unlist(lapply(xec, "[[", 1)))
xec = lapply(xec, setkey, "n", "n.fi")

save(xec, file = file.path(local_out_folder, "datasets", "extcts.RData"))

## For some reason getting multiple identical keys - because not summing!! 
xec = lapply(xec, function(d) { d[, list(count = sum(count)), by = list(n, n.fi)] })
    
## Props with no addons. 
no.ao = lapply(xec, function(d) { d[list(0,0)] })
no.ao = rbindlist(lapply(names(no.ao), function(nn) { 
    data.table(group = nn, nprof = no.ao[[nn]][, count])
}))
setkey(no.ao, group)
setkey(group.sizes, group)
no.ao = no.ao[group.sizes, list(nprof, nprof.gp = count)]
no.ao[, prop := round(nprof/nprof.gp, 5)]


###################################################


## { table = "spvcount", grp, nspv, nswp, nsap } -> count
## { table = "spvtype", grp, spvt, prop } -> count


## Search provider stats. 
zsp = rhwatch(
    map = function(k,r) { 
        if(!(k$table %in% c("spvcount", "spvtype"))) return()
        kk = k[["table"]]
        k[["table"]] = NULL
        rhcollect(kk, c(k, count = r))
    }, 
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = out.grpstats, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)
    
xsp = rhread(zsp)
xsp = setNames(lapply(xsp, "[[", 2), sapply(xsp, "[[", 1))

save(xsp, file = file.path(local_out_folder, "datasets", "spvcts.RData"))


#####

## { table = "positivesc", grp, ppsc, vtype, swsc } -> count

zps = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "positivesc")) return()
        k[["table"]] = NULL
        rhcollect(1, c(k, count = r))
    }, 
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = out.grpstats, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)

xps = rhread(zps)
xps = xps[[1]][[2]]

save(xps, file = file.path(local_out_folder, "datasets", "possc.RData"))
   
   
    
###################################################


## { table = "info", grp, mver, cver, age, geo } -> count 

zi = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "info")) return()
        kk = k[["grp"]]
        k[c("table", "grp")] = NULL
        rhcollect(kk, c(k, count = r))
    }, 
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = out.grpstats, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)
    
xi = rhread(zi)
xi = setNames(lapply(xi, "[[", 2), sapply(xi, "[[", 1))

save(xi, file = file.path(local_out_folder, "datasets", "info.RData"))


#####################################################


## Re-collect activity measures. 


m = function(k,r) {
    ## Default status
    defst = if(!is.null(r$def.all)) r$def.all else mean(r$def) >= 0.5
    ## Total search counts
    sctot = sum(unlist(lapply(r$sc, "[", , "ct")))
    ## Total active ticks. 
    attot = sum(unlist(lapply(r$adays, "[[", "clt"))) + sum(unlist(lapply(r$adays, "[[", "abt")))
    
    ## Identify search group
    sgrp = get.sgp(defst, sctot, attot)
    
    rhcounter("SEARCH_GROUP", sgrp, 1)
    
    #####
    
    ## Startup 
    
    ## Median firstPaint time. 
    fp = unlist(lapply(r$adays, "[[", "fp"))
    ## Remove negatives. 
    mfp = if(is.null(fp)) { NA } else { median(fp[fp > 0]) }
    
    rhcollect(list(table = "startup", grp = sgrp,
                    mfp = mfp), 1)
    
    
    #####
    
    ## Activity
    
    ## Number of days
    ndays = length(r$adays)
    
    ## Number of sessions
    nsess = length(fp)
    
    rhcollect(list(table = "activitycounts", grp = sgrp,
                    nd = ndays,
                    ns = nsess,
                    tat = attot), 1)
    
    ## Mean number of sessions per day. 
    sess.day = round(mean(unlist(lapply(r$adays, function(s) { 
        length(s$fp) 
    }))), 2)
    
    ## Median number of (clean) active ticks per session.
    at.sess = median(unlist(lapply(r$adays, "[[", "clt")))
    
    rhcollect(list(table = "activityrates", grp = sgrp,
                    sesspd = sess.day,
                    atps = at.sess), 1)
                
    }


out.grpstats5 = file.path(out_folder, "group-stats-act")

param = list(get.sgp = group.prof)

    
z = rhwatch(map = m, reduce = summer, 
    input = out.data, output = out.grpstats5, 
    param = param, 
    setup = expression(map = { suppressPackageStartupMessages(library(data.table)) }), 
    debug = "count", read = FALSE)


## Activity measures. 

zarm = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "activityrates")) return()
        kk = k[["grp"]]
        k[c("table", "grp")] = NULL
        rhcollect(kk, c(k, count = r))
    }, 
    input = out.grpstats5, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), read = FALSE)
    
zarr = rhwatch(map = function(k,r) { 
        rhcollect(k, lapply(r, isn)) 
    },
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = zarm, 
    param = list(isn = isn),
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), read = FALSE)

xar = rhread(zarr)
xar = setNames(lapply(xar, "[[", 2), sapply(xar, "[[", 1))

## Marginal quantiles of sessions per day and active ticks per session. 
xarm = rbindlist(lapply(names(xar), function(nn) {
    data.table(grp = nn, p = 1:1000/1000,
        spd = wtd.quantile(xar[[nn]][, sesspd], w = xar[[nn]][, count], 
            p = 1:1000/1000), 
        aps = wtd.quantile(xar[[nn]][, atps], w = xar[[nn]][, count], 
            p = 1:1000/1000))
}))

## Joint conditional quantiles. 
xarj = lapply(xar, function(r) {    
    r = r[sesspd > 0]
    sm = r[, wtd.quantile(sesspd, w = count, p = 1:500/500)]
    usm = unique(sm)
    r[, bsm := cut(sesspd, breaks = c(0, usm), labels = usm)]
    aq = lapply(r[, levels(bsm)], function(lv) {
        r[bsm == lv, wtd.quantile(atps, w = count, p = 1:500/500)]
    })
    names(aq) = r[, levels(bsm)]
    rbindlist(lapply(sm, function(s) {
        data.table(spd = s, aps = aq[[as.character(s)]])
    }))
})

save(xarm, xarj, file = file.path(local_out_folder, "datasets","arates.RData"))


## Startup times. 

zfp = rhwatch(
    map = function(k,r) { 
        if(!identical(k$table, "startup")) return()
        kk = k[["grp"]]
        k[c("table", "grp")] = NULL
        rhcollect(kk, c(k, count = r))
    }, 
    reduce = rhoptions()$templates$raggregate({
        adata = unlist(adata, recursive = FALSE)
        adata = lapply(adata, function(d) { 
            if(!is.data.table(d)) as.data.table(d) else d
        })
        rhcollect(reduce.key, rbindlist(adata))
    }, combine = TRUE), 
    input = out.grpstats5, 
    setup = expression({ suppressPackageStartupMessages(library(data.table)) }), 
    read = FALSE)
    
xfp = rhread(zfp)
xfp = setNames(lapply(xfp, "[[", 2), sapply(xfp, "[[", 1))

save(xfp, file = file.path(local_out_folder, "datasets", "fp.RData"))



