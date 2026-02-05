#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(ncdf4)
  library(SPEI)
  library(optparse)
})

# Command line arguments
option_list <- list(
  make_option("--precip", type="character", help="Precipitation NetCDF files (comma-separated)"),
  make_option("--tas", type="character", default=NULL, help="Mean temperature files"),
  make_option("--tasmin", type="character", help="Min temperature files"),
  make_option("--tasmax", type="character", help="Max temperature files"),
  make_option("--hurs", type="character", default=NULL, help="Relative humidity files"),
  make_option("--rsds", type="character", default=NULL, help="Solar radiation files"),
  make_option("--sfcwind", type="character", default=NULL, help="Wind speed files"),
  make_option("--ps", type="character", default=NULL, help="Surface pressure files"),
  make_option("--pet-method", type="character", default="penman", help="PET method: hargreaves, thornthwaite, penman"),
  make_option("--scales", type="character", default="2,3,6", help="SPEI scales (e.g., '2,3,6')"),
  make_option("--calibration", type="character", default="1979-2014", help="Calibration period (YYYY-YYYY)"),
  make_option("--out-pet", type="character", default=NULL, help="Output PET NetCDF file (optional)"),
  make_option("--out-spei", type="character", help="Output SPEI NetCDF file")
)

opt <- parse_args(OptionParser(option_list=option_list))

# Parse inputs
scales <- as.integer(strsplit(opt$scales, ",")[[1]])
cal_years <- as.integer(strsplit(opt$calibration, "-")[[1]])

cat("==============================================\n")
cat("ISIMIP SPEI - R Implementation\n")
cat("==============================================\n")
cat(sprintf("PET method: %s\n", opt$`pet-method`))
cat(sprintf("SPEI scales: %s\n", paste(scales, collapse=", ")))
cat(sprintf("Calibration: %d-%d\n", cal_years[1], cal_years[2]))
cat("==============================================\n\n")

# Helper function to load NetCDF variable
load_var <- function(file_pattern, varname=NULL) {
  files <- unlist(strsplit(file_pattern, ","))
  
  if (length(files) == 1) {
    nc <- nc_open(files[1])
    if (is.null(varname)) varname <- names(nc$var)[1]
    data <- ncvar_get(nc, varname)
    time <- ncvar_get(nc, "time")
    lon <- ncvar_get(nc, "lon")
    lat <- ncvar_get(nc, "lat")
    nc_close(nc)
    return(list(data=data, time=time, lon=lon, lat=lat, varname=varname))
  } else {
    # Multiple files - concatenate along time
    all_data <- list()
    all_time <- c()
    
    for (f in files) {
      nc <- nc_open(f)
      if (is.null(varname)) varname <- names(nc$var)[1]
      all_data[[length(all_data)+1]] <- ncvar_get(nc, varname)
      all_time <- c(all_time, ncvar_get(nc, "time"))
      if (length(all_data) == 1) {
        lon <- ncvar_get(nc, "lon")
        lat <- ncvar_get(nc, "lat")
      }
      nc_close(nc)
    }
    
    data <- do.call(abind::abind, c(all_data, list(along=3)))
    return(list(data=data, time=all_time, lon=lon, lat=lat, varname=varname))
  }
}

# ============================================================================
# STEP 1: Load data and compute PET
# ============================================================================

cat("Loading data...\n")

pr <- load_var(opt$precip, "pr")
tasmin <- load_var(opt$tasmin, "tasmin")
tasmax <- load_var(opt$tasmax, "tasmax")

# Get time units from first file
pr_files <- unlist(strsplit(opt$precip, ","))
nc_temp <- nc_open(pr_files[1])
time_units <- ncatt_get(nc_temp, "time", "units")
nc_close(nc_temp)

if (!time_units$hasatt) {
  cat("  WARNING: No time units attribute found, assuming 'days since 1850-01-01'\n")
  time_origin <- "1850-01-01"
} else {
  time_units_str <- time_units$value
  cat(sprintf("  NetCDF time units: %s\n", time_units_str))
  
  # Extract origin date from string like "days since YYYY-MM-DD"
  origin_match <- regmatches(time_units_str, regexpr("\\d{4}-\\d{2}-\\d{2}", time_units_str))
  if (length(origin_match) > 0) {
    time_origin <- origin_match[1]
  } else {
    time_origin <- "1850-01-01"
    cat("  WARNING: Could not parse origin, using 1850-01-01\n")
  }
  cat(sprintf("  Using time origin: %s\n", time_origin))
}

# Convert dimensions
n_lon <- length(pr$lon)
n_lat <- length(pr$lat)
n_time <- dim(pr$data)[3]

cat(sprintf("  Grid: %d lon x %d lat x %d time\n", n_lon, n_lat, n_time))

# Convert units and aggregate to monthly
cat("Converting to monthly data...\n")

# Precip: kg/m2/s -> mm/month (sum over days in month)
# Temperature: K -> C, monthly mean
# First check if data is daily or already monthly
time_diff <- median(diff(pr$time))
is_daily <- time_diff < 5  # Less than 5 days between timesteps = daily

cat(sprintf("  Time values range: %.1f to %.1f\n", min(pr$time), max(pr$time)))
cat(sprintf("  Time step difference: %.1f days\n", time_diff))
cat(sprintf("  Is daily: %s\n", is_daily))

if (is_daily) {
  cat("  Detected daily data, aggregating to monthly...\n")
  
  # Create monthly time index using correct origin
  dates <- as.Date(pr$time, origin=time_origin)
  
  cat(sprintf("  First date parsed: %s\n", as.character(dates[1])))
  cat(sprintf("  Last date parsed: %s\n", as.character(dates[length(dates)])))
  
  year_month <- format(dates, "%Y-%m")
  unique_months <- unique(year_month)
  n_months <- length(unique_months)
  
  # Create monthly date objects (first day of each month)
  monthly_dates <- as.Date(paste0(unique_months, "-01"))
  
  cat(sprintf("  Monthly dates range: %s to %s\n", 
             as.character(monthly_dates[1]), 
             as.character(monthly_dates[length(monthly_dates)])))
  
  # Initialize monthly arrays
  pr_monthly <- array(0, dim=c(n_lon, n_lat, n_months))
  tasmin_monthly <- array(NA, dim=c(n_lon, n_lat, n_months))
  tasmax_monthly <- array(NA, dim=c(n_lon, n_lat, n_months))
  
  cat(sprintf("  Aggregating %d daily timesteps to %d months...\n", n_time, n_months))
  
  # Aggregate by month
  for (m in seq_along(unique_months)) {
    if (m %% 12 == 0) cat(sprintf("    Month %d/%d\n", m, n_months))
    
    month_mask <- year_month == unique_months[m]
    n_days <- sum(month_mask)
    
    # Precip: sum over month (kg/m2/s * 86400 s/day * days)
    pr_monthly[, , m] <- apply(pr$data[, , month_mask, drop=FALSE] * 86400, c(1,2), sum)
    
    # Temperature: monthly mean
    tasmin_monthly[, , m] <- apply(tasmin$data[, , month_mask, drop=FALSE], c(1,2), mean, na.rm=TRUE)
    tasmax_monthly[, , m] <- apply(tasmax$data[, , month_mask, drop=FALSE], c(1,2), mean, na.rm=TRUE)
  }
  
  # Update arrays
  pr_mm <- pr_monthly
  tasmin$data <- tasmin_monthly
  tasmax$data <- tasmax_monthly
  n_time <- n_months
  
  cat("  Aggregation complete!\n")
  
} else {
  cat("  Data already monthly\n")
  # Precip: kg/m2/s -> mm/month (approximate)
  pr_mm <- pr$data * 86400 * 30  # Rough estimate
  
  # Create monthly_dates from original time using correct origin
  dates <- as.Date(pr$time, origin=time_origin)
  monthly_dates <- dates
}

# Temperature: K -> C if needed
if (mean(tasmin$data, na.rm=TRUE) > 100) {
  tasmin$data <- tasmin$data - 273.15
  tasmax$data <- tasmax$data - 273.15
}

cat(sprintf("\nComputing PET (%s method)...\n", opt$`pet-method`))

# Initialize PET array
pet_data <- array(NA, dim=c(n_lon, n_lat, n_time))

# Compute PET per grid cell
for (i in 1:n_lon) {
  if (i %% 5 == 0) cat(sprintf("  Longitude %d/%d\n", i, n_lon))
  
  for (j in 1:n_lat) {
    tmin_ts <- tasmin$data[i, j, ]
    tmax_ts <- tasmax$data[i, j, ]
    
    if (all(is.na(tmin_ts))) next
    
    # Latitude for this cell
    lat_val <- pr$lat[j]
    
    # Compute PET based on method
    pet_ts <- tryCatch({
      if (opt$`pet-method` == "hargreaves") {
        hargreaves(Tmin=tmin_ts, Tmax=tmax_ts, lat=lat_val, verbose=FALSE)
        
      } else if (opt$`pet-method` == "thornthwaite") {
        tas_ts <- (tmin_ts + tmax_ts) / 2
        thornthwaite(Tave=tas_ts, lat=lat_val, verbose=FALSE)
        
      } else if (opt$`pet-method` == "penman") {
        # Penman requires more variables - load if not already loaded
        if (!exists("hurs")) {
          hurs <<- load_var(opt$hurs, "hurs")
          rsds <<- load_var(opt$rsds, "rsds")
          sfcwind <<- load_var(opt$sfcwind, "sfcwind")
        }
        
        # Convert Rs from W/m2 to MJ/m2/day
        rs_val <- rsds$data[i, j, ] * 0.0864
        
        penman(
          Tmin=tmin_ts, 
          Tmax=tmax_ts,
          U2=sfcwind$data[i, j, ],
          Rs=rs_val,
          RH=hurs$data[i, j, ],
          lat=lat_val,
          verbose=FALSE
        )
      }
    }, error = function(e) {
      rep(NA, length(tmin_ts))
    })
    
    pet_data[i, j, ] <- as.numeric(pet_ts)
  }
}

# Save PET if requested
if (!is.null(opt$`out-pet`)) {
  cat(sprintf("\nSaving PET to: %s\n", opt$`out-pet`))
  
  lon_dim <- ncdim_def("lon", "degrees_east", pr$lon)
  lat_dim <- ncdim_def("lat", "degrees_north", pr$lat)
  
  # Create monthly time dimension (use first day of each month)
  if (is_daily) {
    time_vals <- as.numeric(monthly_dates - as.Date(time_origin))
    time_dim <- ncdim_def("time", paste("days since", time_origin), time_vals)
  } else {
    time_dim <- ncdim_def("time", paste("days since", time_origin), pr$time)
  }
  
  pet_var <- ncvar_def("pet", "mm/month", list(lon_dim, lat_dim, time_dim), -999,
                       longname=sprintf("Potential ET (%s)", opt$`pet-method`))
  
  nc_out <- nc_create(opt$`out-pet`, pet_var)
  
  # Replace NaN/Inf with missing value
  pet_clean <- pet_data
  pet_clean[!is.finite(pet_clean)] <- -999
  
  ncvar_put(nc_out, "pet", pet_clean)
  ncatt_put(nc_out, 0, "title", sprintf("PET (%s method)", opt$`pet-method`))
  nc_close(nc_out)
}

# ============================================================================
# STEP 2: Compute SPEI
# ============================================================================

cat("\nComputing SPEI...\n")

# Water balance
wb <- pr_mm - pet_data

# Save water balance if PET was saved
if (!is.null(opt$`out-pet`)) {
  wb_file <- sub("pet_", "wb_", opt$`out-pet`)
  cat(sprintf("Saving water balance to: %s\n", wb_file))
  
  wb_var <- ncvar_def("wb", "mm/month", list(lon_dim, lat_dim, time_dim), -999,
                      longname="Water Balance (P - PET)")
  nc_wb <- nc_create(wb_file, wb_var)
  
  # Replace NaN/Inf with missing value
  wb_clean <- wb
  wb_clean[!is.finite(wb_clean)] <- -999
  
  ncvar_put(nc_wb, "wb", wb_clean)
  ncatt_put(nc_wb, 0, "title", "Water Balance (Precipitation - PET)")
  ncatt_put(nc_wb, 0, "pet_method", opt$`pet-method`)
  nc_close(nc_wb)
}

# Create output list
spei_results <- list()

for (scale in scales) {
  cat(sprintf("\nSPEI-%d:\n", scale))
  
  spei_out <- array(NA, dim=c(n_lon, n_lat, n_time))
  
  # Track success rate
  n_success <- 0
  n_attempts <- 0
  first_error <- NULL
  
  for (i in 1:n_lon) {
    if (i %% 5 == 0) cat(sprintf("  Longitude %d/%d\n", i, n_lon))
    
    for (j in 1:n_lat) {
      wb_ts <- wb[i, j, ]
      
      if (all(is.na(wb_ts))) next
      
      n_attempts <- n_attempts + 1
      
      # Convert to monthly ts object with correct start date
      start_year <- as.integer(format(monthly_dates[1], "%Y"))
      start_month <- as.integer(format(monthly_dates[1], "%m"))
      
      wb_ts_obj <- ts(wb_ts, frequency=12, start=c(start_year, start_month))
      
      # Debug first cell
      if (i == 1 && j == 1 && scale == scales[1]) {
        cat(sprintf("    First cell: start=%d-%02d, length=%d months\n", 
                   start_year, start_month, length(wb_ts)))
        cat(sprintf("    WB range: %.1f to %.1f mm/month\n", 
                   min(wb_ts, na.rm=TRUE), max(wb_ts, na.rm=TRUE)))
      }
      
      tryCatch({
        spei_calc <- spei(
          wb_ts_obj,
          scale = scale,
          ref.start = c(cal_years[1], 1),
          ref.end = c(cal_years[2], 12),
          na.rm = TRUE,
          verbose = FALSE
        )
        
        spei_out[i, j, ] <- as.numeric(spei_calc$fitted)
        n_success <- n_success + 1
        
      }, error = function(e) {
        # Save first error
        if (is.null(first_error)) {
          first_error <<- e$message
        }
      })
    }
  }
  
  success_rate <- 100 * n_success / n_attempts
  cat(sprintf("  Success rate: %.1f%% (%d/%d cells)\n", success_rate, n_success, n_attempts))
  
  if (!is.null(first_error)) {
    cat(sprintf("  Example error: %s\n", first_error))
  }
  
  spei_results[[sprintf("spei_%02d", scale)]] <- spei_out
  
  # Check how many valid values we got
  n_valid <- sum(is.finite(spei_out))
  n_total <- length(spei_out)
  pct_valid <- 100 * n_valid / n_total
  
  cat(sprintf("  Computed: %.1f%% valid values\n", pct_valid))
  
  if (pct_valid < 10) {
    cat(sprintf("  WARNING: Less than 10%% valid SPEI values! Check your data.\n"))
  }
}

# ============================================================================
# STEP 3: Save SPEI output
# ============================================================================

cat(sprintf("\nSaving SPEI to: %s\n", opt$`out-spei`))

lon_dim <- ncdim_def("lon", "degrees_east", pr$lon)
lat_dim <- ncdim_def("lat", "degrees_north", pr$lat)

# Use monthly time dimension
if (is_daily) {
  monthly_dates <- as.Date(paste0(unique_months, "-01"))
  time_vals <- as.numeric(monthly_dates - as.Date(time_origin))
  time_dim <- ncdim_def("time", paste("days since", time_origin), time_vals)
} else {
  time_dim <- ncdim_def("time", paste("days since", time_origin), pr$time)
}

nc_vars <- list()
for (scale in scales) {
  var_name <- sprintf("spei_%02d", scale)
  nc_vars[[var_name]] <- ncvar_def(
    var_name, "1", list(lon_dim, lat_dim, time_dim), -999,
    longname=sprintf("SPEI %d-month", scale)
  )
}

nc_out <- nc_create(opt$`out-spei`, nc_vars)

for (scale in scales) {
  var_name <- sprintf("spei_%02d", scale)
  
  # Replace NaN/Inf with missing value before writing
  spei_data <- spei_results[[var_name]]
  spei_data[!is.finite(spei_data)] <- -999
  
  # Count valid values for debugging
  n_valid <- sum(is.finite(spei_results[[var_name]]))
  n_total <- length(spei_results[[var_name]])
  pct_valid <- 100 * n_valid / n_total
  
  cat(sprintf("  %s: %.1f%% valid values (%d/%d)\n", 
             var_name, pct_valid, n_valid, n_total))
  
  ncvar_put(nc_out, var_name, spei_data)
}

ncatt_put(nc_out, 0, "title", "SPEI (R SPEI package)")
ncatt_put(nc_out, 0, "calibration_period", opt$calibration)
ncatt_put(nc_out, 0, "pet_method", opt$`pet-method`)

nc_close(nc_out)

cat("\nDone!\n")