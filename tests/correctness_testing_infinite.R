suppressMessages(library(Rcpp))
suppressMessages(library(dplyr))
suppressMessages(library(data.table))
suppressMessages(library(qs))
options(warn=1)

Sys.setenv("PKG_CXXFLAGS"="-std=c++11")
cppFunction("CharacterVector splitstr(std::string x, std::vector<double> cuts){
            CharacterVector ret(cuts.size() - 1);
            for(uint64_t i=1; i<cuts.size(); i++) {
              ret[i-1] = x.substr(std::round(cuts[i-1])-1, std::round(cuts[i])-std::round(cuts[i-1]));
            }
            return ret;
            }")

obj_size <- 0; max_size <- 1e7
get_obj_size <- function() {
  get("obj_size", envir=globalenv())
}
set_obj_size <- function(x) {
  assign("obj_size", get_obj_size() + as.numeric(object.size(x)), envir=globalenv())
  return(get_obj_size());
}
random_object_generator <- function(N) { # additional input: global obj_size, max_size
  ret <- as.list(1:N)
  for(i in 1:N) {
    if(get_obj_size() > get("max_size", envir=globalenv())) break;
    otype <- sample(12, size=1)
    z <- NULL
    is_attribute <- ifelse(i == 1, F, sample(c(F,T),size=1))
    if(otype == 1) {z <- rnorm(sample(1e4,1)); set_obj_size(z);}
    else if(otype == 2) { z <- sample(sample(1e4,1))-5e2; set_obj_size(z); }
    else if(otype == 3) { z <- sample(c(T,F,NA), size=sample(1e4,1), replace=T); set_obj_size(z); }
    else if(otype == 4) { z <- (sample(256, size=sample(1e4,1), replace=T)-1) %>% as.raw; set_obj_size(z); }
    else if(otype == 5) { z <- replicate(sample(sample(1e4,1),size=1), {rep(letters, length.out=sample(10, size=1)) %>% paste(collapse="")}); set_obj_size(z); }
    else if(otype == 6) { z <- rep(letters, length.out=sample(sample(1e4,1), size=1)) %>% paste(collapse=""); set_obj_size(z); }
    else if(otype == 7) { z <- as.formula("y ~ a + b + c : d", env=globalenv()); attr(z, "blah") <-sample(sample(1e4,1))-5e2; set_obj_size(z); }
    else { z <- random_object_generator(N) }
    if(is_attribute) {
      attr(ret[[i-1]], runif(1) %>% as.character()) <- z
    } else {
      ret[[i]] <- z
    }
  }
  return(ret)
}

test_points <- c(0, 1,2,4,8, 2^5-1, 2^5+1, 2^5,2^8-1, 2^8+1,2^8,2^16-1, 2^16+1, 2^16, 1e6, 1e7)
extra_test_points <- c(2^32-1, 2^32+1, 2^32) # not enough memory on desktop
reps <- 1e6

################################################################################################

qsave_rand <- function(x, file) {
  qsave(x, file="/tmp/ctest.z", preset = "custom", algorithm = sample(c("lz4", "zstd", "lz4hc", "zstd_stream"), 1),
        compress_level=sample(10,1), shuffle_control = sample(0:15,1), nthreads=sample(5,1) )
}

qread_rand <- function(x, file) {
  qread("/tmp/ctest.z", 
        use_alt_rep = sample(c(T,F),1),inspect=T,nthreads=sample(5,1))
}

for(q in 1:reps) {
  set.seed(1)
  
  # String correctness
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- rep(letters, length.out=tp) %>% paste(collapse="")
      x1 <- c(NA, "", x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("strings: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # Character vectors
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      # qs_use_alt_rep(F)
      x1 <- rep(as.raw(sample(255)), length.out = tp*10) %>% rawToChar
      cuts <- sample(tp*10, tp+1) %>% sort %>% as.numeric
      x1 <- splitstr(x1, cuts)
      x1 <- c(NA, "", x1)
      qsave_rand(x1, file="/tmp/test.z")
      time[i] <- Sys.time()
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Character Vectors: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # Integers
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- sample(1:tp, replace=T)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Integers: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # Doubles
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- rnorm(tp)
      x1 <- c(NA, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Numeric: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # Logical
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      x1 <- sample(c(T,F,NA), replace=T, size=tp)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Logical: %s, %s s",tp, signif(mean(time),4)))
  }
  
  for(i in 1:3) {
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- data.frame(str=x1,num = runif(1:1000), stringsAsFactors = F)
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    gc()
    stopifnot(identical(z, x1))
  }
  print("Data.frame test")
  
  for(i in 1:3) {
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- data.table(str=x1,num = runif(1:1e6))
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    gc()
    stopifnot(all(z==x1))
  }
  print("Data.table test")
  
  for(i in 1:3) {
    x1 <- rep( replicate(1000, { rep(letters, length.out=2^7+sample(10, size=1)) %>% paste(collapse="") }), length.out=1e6 )
    x1 <- tibble(str=x1,num = runif(1:1e6))
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    gc()
    stopifnot(identical(z, x1))
  }
  print("Tibble test")
  
  # Encoding test
  if (Sys.info()[['sysname']] != "Windows") {
    for(i in 1:3) {
      
      x1 <- "己所不欲，勿施于人" # utf 8
      x2 <- x1
      Encoding(x2) <- "latin1"
      x3 <- x1
      Encoding(x3) <- "bytes"
      x4 <- rep(x1, x2, length.out=1e4) %>% paste(collapse=";")
      x1 <- c(x1, x2, x3, x4)
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      gc()
      stopifnot(identical(z, x1))
    }
    print("Encoding test")
  } else {
    print("(Encoding test not run on windows)")
  }
  
  # complex vectors
  time <- vector("numeric", length=3)
  for(tp in test_points) {
    for(i in 1:3) {
      
      re <- rnorm(tp)
      im <- runif(tp)
      x1 <- complex(real=re, imaginary=im)
      x1 <- c(NA_complex_, x1)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Complex: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # factors
  for(tp in test_points) {
    time <- vector("numeric", length=3)
    for(i in 1:3) {
      x1 <- factor(rep(letters, length.out=tp), levels=sample(letters), ordered=TRUE)
      time[i] <- Sys.time()
      qsave_rand(x1, file="/tmp/test.z")
      z <- qread_rand(file="/tmp/test.z")
      time[i] <- Sys.time() - time[i]
      gc()
      stopifnot(identical(z, x1))
    }
    print(sprintf("Factors: %s, %s s",tp, signif(mean(time),4)))
  }
  
  # nested lists
  time <- vector("numeric", length=8)
  for(i in 1:8) {
    # qs_use_alt_rep(sample(c(T,F), size=1))
    obj_size <- 0
    x1 <- random_object_generator(12)
    print(sprintf("Nested list/attributes: %s bytes", object.size(x1) %>% as.numeric))
    time[i] <- Sys.time()
    qsave_rand(x1, file="/tmp/test.z")
    z <- qread_rand(file="/tmp/test.z")
    time[i] <- Sys.time() - time[i]
    gc()
    stopifnot(identical(z, x1))
  }
  print(sprintf("Nested list/attributes: %s s", signif(mean(time),4)))
}
