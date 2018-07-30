package com.axibase.tsd.model.version;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;


/**
 * Contains information about ATSD version such as build, license, server date settings.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtsdVersion {
    private BuildInfo buildInfo;
    private License license;
    private DateInfo date;
    private Settings settings;
}
