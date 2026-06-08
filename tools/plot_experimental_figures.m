% plot_experimental_figures
% Build the two Experimental Results figures from Web Intercom metrics workbooks.
%
% Usage from MATLAB:
%   cd D:\webrtc_tailscale
%   run tools\plot_experimental_figures.m
%
% The script preserves the raw workbook and filters throw-away measurement
% records only in the plotting pipeline:
%   - Time Series rows with no active clients and no MOS/latency/jitter signal.
%   - Client Summary rows shorter than minSessionSeconds or with no media data.
%
% Output:
%   local_figures\experimental_results\fig1_jitter_cdf.(png,pdf,fig)
%   local_figures\experimental_results\fig2_mos_latency.(png,pdf,fig)
%   local_figures\experimental_results\cleaned_data\*_clean.csv

clear;
clc;

scriptDir = fileparts(mfilename("fullpath"));
projectRoot = fileparts(scriptDir);

minSessionSeconds = 30;
outputDir = fullfile(projectRoot, "local_figures", "experimental_results");
cleanDir = fullfile(outputDir, "cleaned_data");

if ~exist(outputDir, "dir")
    mkdir(outputDir);
end
if ~exist(cleanDir, "dir")
    mkdir(cleanDir);
end

inputFiles = collectMetricFiles(projectRoot);
if isempty(inputFiles)
    error("No metrics workbook found. Put web_intercom_metrics.xlsx or metrics_*.xlsx in the project root, data, or metrics folder.");
end

runs = struct( ...
    "file", {}, ...
    "label", {}, ...
    "mediaMode", {}, ...
    "rawTimeSeriesRows", {}, ...
    "cleanTimeSeriesRows", {}, ...
    "rawClientRows", {}, ...
    "cleanClientRows", {}, ...
    "jitterX", {}, ...
    "jitterY", {}, ...
    "jitterSource", {}, ...
    "avgMos", {}, ...
    "p95LatencyMs", {}, ...
    "qoeSource", {});

for k = 1:numel(inputFiles)
    run = loadMetricRun(inputFiles(k), minSessionSeconds, cleanDir);
    if ~isempty(run.jitterX) || isfinite(run.avgMos) || isfinite(run.p95LatencyMs)
        runs(end + 1) = run; %#ok<SAGROW>
    end
end

if isempty(runs)
    error("Metrics workbooks were found, but no usable measured data remained after cleaning.");
end

printCleaningReport(runs);
plotJitterCdf(runs, outputDir);
plotMosLatency(runs, outputDir);

fprintf("\nDone. Figures saved in:\n  %s\n", outputDir);

function files = collectMetricFiles(projectRoot)
    patterns = [
        fullfile(projectRoot, "*metrics*.xlsx")
        fullfile(projectRoot, "data", "*.xlsx")
        fullfile(projectRoot, "metrics", "*.xlsx")
    ];

    paths = strings(0, 1);
    for p = 1:numel(patterns)
        listing = dir(patterns(p));
        for i = 1:numel(listing)
            if listing(i).isdir || startsWith(listing(i).name, "~$")
                continue;
            end
            paths(end + 1, 1) = string(fullfile(listing(i).folder, listing(i).name)); %#ok<AGROW>
        end
    end

    if isempty(paths)
        files = strings(0, 1);
        return;
    end

    [~, keep] = unique(paths, "stable");
    files = paths(sort(keep));
end

function run = loadMetricRun(filePath, minSessionSeconds, cleanDir)
    timeSeries = readSheetSafe(filePath, "Time Series");
    clientSummary = readSheetSafe(filePath, "Client Summary");
    jitterCdf = readSheetSafe(filePath, "Jitter CDF");
    qoeSummary = readSheetSafe(filePath, "QoE Summary");

    cleanTimeSeries = cleanTimeSeriesRows(timeSeries);
    cleanClients = cleanClientRows(clientSummary, minSessionSeconds);

    [~, stem] = fileparts(filePath);
    cleanStem = safeStem(stem);
    if ~isempty(cleanTimeSeries)
        writetable(cleanTimeSeries, fullfile(cleanDir, cleanStem + "_time_series_clean.csv"));
    end
    if ~isempty(cleanClients)
        writetable(cleanClients, fullfile(cleanDir, cleanStem + "_client_summary_clean.csv"));
    end

    mediaMode = inferMediaMode(cleanClients, clientSummary);
    [jitterX, jitterY, jitterSource] = getJitterCdf(jitterCdf, cleanTimeSeries, cleanClients);
    [avgMos, p95LatencyMs, qoeSource] = getQoeMetrics(cleanTimeSeries, cleanClients, qoeSummary);

    run.file = string(filePath);
    run.label = makeScenarioLabel(filePath, mediaMode);
    run.mediaMode = mediaMode;
    run.rawTimeSeriesRows = height(timeSeries);
    run.cleanTimeSeriesRows = height(cleanTimeSeries);
    run.rawClientRows = height(clientSummary);
    run.cleanClientRows = height(cleanClients);
    run.jitterX = jitterX;
    run.jitterY = jitterY;
    run.jitterSource = jitterSource;
    run.avgMos = avgMos;
    run.p95LatencyMs = p95LatencyMs;
    run.qoeSource = qoeSource;
end

function T = readSheetSafe(filePath, sheetName)
    try
        T = readtable(filePath, "Sheet", sheetName, "VariableNamingRule", "preserve", "TextType", "string");
    catch err
        warning("Could not read sheet '%s' from %s: %s", sheetName, filePath, err.message);
        T = table();
    end
end

function clean = cleanTimeSeriesRows(T)
    if isempty(T)
        clean = T;
        return;
    end

    active = colnum(T, "active_clients");
    mos = colnum(T, "browser_avg_estimated_mos");
    latency = colnum(T, "browser_avg_playout_latency_ms");
    jitter = colnum(T, "browser_max_jitter_ms");

    hasMeasurementSignal = active > 0 | mos > 0 | latency > 0 | jitter > 0;
    mosLooksValid = isnan(mos) | mos == 0 | (mos >= 1 & mos <= 4.5);
    latencyLooksValid = isnan(latency) | latency == 0 | latency > 0;
    clean = T(hasMeasurementSignal & mosLooksValid & latencyLooksValid, :);
end

function clean = cleanClientRows(T, minSessionSeconds)
    if isempty(T)
        clean = T;
        return;
    end

    duration = colnum(T, "session_duration_seconds");
    received = colnum(T, "received_packets");
    played = colnum(T, "played_packets");
    webrtcRtt = colnum(T, "webrtc_rtt_ms");
    relayJitter = colnum(T, "rfc3550_jitter_ms");
    webrtcJitter = colnum(T, "webrtc_jitter_ms");
    mos = colnum(T, "estimated_mos");

    longEnough = duration >= minSessionSeconds;
    hasMediaEvidence = received > 0 | played > 0 | webrtcRtt > 0 | relayJitter > 0 | webrtcJitter > 0;
    mosLooksValid = isnan(mos) | mos == 0 | (mos >= 1 & mos <= 4.5);
    clean = T(longEnough & hasMediaEvidence & mosLooksValid, :);
end

function [x, y, source] = getJitterCdf(jitterCdf, cleanTimeSeries, cleanClients)
    pct = colnum(jitterCdf, "Percentile");
    jitter = colnum(jitterCdf, "Jitter_ms");
    keep = isfinite(pct) & isfinite(jitter) & pct >= 0 & pct <= 100 & jitter >= 0;

    if nnz(keep) >= 2 && max(jitter(keep)) > 0
        x = jitter(keep);
        y = pct(keep);
        [x, order] = sort(x);
        y = y(order);
        source = "Jitter CDF sheet";
        return;
    end

    values = [
        colnum(cleanTimeSeries, "browser_max_jitter_ms")
        colnum(cleanClients, "rfc3550_jitter_ms")
        colnum(cleanClients, "webrtc_jitter_ms")
    ];
    values = values(isfinite(values) & values > 0);
    [x, y] = empiricalCdf(values);
    source = "Cleaned Time Series / Client Summary";
end

function [avgMos, p95LatencyMs, source] = getQoeMetrics(cleanTimeSeries, cleanClients, qoeSummary)
    mos = colnum(cleanTimeSeries, "browser_avg_estimated_mos");
    mos = mos(isfinite(mos) & mos >= 1 & mos <= 4.5);
    latency = colnum(cleanTimeSeries, "browser_avg_playout_latency_ms");
    latency = latency(isfinite(latency) & latency > 0);
    source = "Cleaned Time Series";

    if isempty(mos)
        mos = colnum(cleanClients, "estimated_mos");
        mos = mos(isfinite(mos) & mos >= 1 & mos <= 4.5);
        source = "Cleaned Client Summary";
    end
    if isempty(latency)
        latency = colnum(cleanClients, "estimated_playout_latency_ms");
        latency = latency(isfinite(latency) & latency > 0);
        source = "Cleaned Client Summary";
    end

    avgMos = meanFinite(mos);
    p95LatencyMs = percentileValue(latency, 95);

    if ~isfinite(avgMos)
        avgMos = summaryMetric(qoeSummary, "Average estimated MOS");
        source = "QoE Summary fallback";
    end
    if ~isfinite(p95LatencyMs)
        p95LatencyMs = summaryMetric(qoeSummary, "Latency p95");
        source = "QoE Summary fallback";
    end
end

function plotJitterCdf(runs, outputDir)
    fig = figure("Color", "w", "Position", [120, 120, 1100, 650]);
    hold on;
    colors = lines(max(numel(runs), 3));

    for k = 1:numel(runs)
        if isempty(runs(k).jitterX)
            continue;
        end
        plot(runs(k).jitterX, runs(k).jitterY, "LineWidth", 2.4, ...
            "Color", colors(k, :), "DisplayName", runs(k).label);
    end

    grid on;
    box on;
    xlabel("Audio jitter (ms)");
    ylabel("CDF (%)");
    title("RFC3550 / WebRTC Audio Jitter CDF");
    ylim([0 100]);
    maxX = max(arrayfun(@(r) maxOrZero(r.jitterX), runs));
    if maxX > 0
        xlim([0 maxX * 1.08]);
    end
    legend("Location", "southeast", "Interpreter", "none");
    applyPaperAxes(gca);
    exportFigure(fig, outputDir, "fig1_jitter_cdf");
end

function plotMosLatency(runs, outputDir)
    labels = string({runs.label});
    mos = [runs.avgMos];
    latency = [runs.p95LatencyMs];
    valid = isfinite(mos) | isfinite(latency);
    labels = labels(valid);
    mos = mos(valid);
    latency = latency(valid);

    if isempty(labels)
        warning("No MOS/latency data available for Figure 2.");
        return;
    end

    fig = figure("Color", "w", "Position", [120, 120, 1100, 650]);
    x = 1:numel(labels);
    mosColor = [0.09 0.50 0.24];
    latencyColor = [0.85 0.32 0.05];

    yyaxis left;
    hMos = bar(x, mos, 0.55, "FaceColor", mosColor, "EdgeColor", "none");
    ylabel("Estimated MOS (1-4.5)");
    ylim([1 4.5]);
    ytickformat("%.1f");

    yyaxis right;
    hLatency = plot(x, latency, "-o", "LineWidth", 2.4, "MarkerSize", 7, ...
        "Color", latencyColor, "MarkerFaceColor", latencyColor);
    ylabel("P95 playout latency (ms)");
    maxLatency = max(latency(isfinite(latency)));
    if ~isempty(maxLatency) && maxLatency > 0
        ylim([0 maxLatency * 1.18]);
    end
    ytickformat("%.0f");

    xlim([0.5 numel(labels) + 0.5]);
    xticks(x);
    xticklabels(labels);
    xtickangle(20);
    grid on;
    box on;
    title("Estimated MOS and P95 Playout Latency");
    legend([hMos, hLatency], ["Average estimated MOS", "P95 playout latency"], ...
        "Location", "northoutside", "Orientation", "horizontal");
    applyPaperAxes(gca);
    ax = gca;
    ax.YAxis(1).Color = mosColor;
    ax.YAxis(2).Color = latencyColor;
    ax.YAxis(2).Exponent = 0;
    addMosLatencyLabels(x, mos, latency, maxLatency, mosColor, latencyColor);
    exportFigure(fig, outputDir, "fig2_mos_latency");
end

function addMosLatencyLabels(x, mos, latency, maxLatency, mosColor, latencyColor)
    yyaxis left;
    for i = 1:numel(x)
        if isfinite(mos(i))
            text(x(i), min(mos(i) + 0.08, 4.45), sprintf("%.2f", mos(i)), ...
                "HorizontalAlignment", "center", "VerticalAlignment", "bottom", ...
                "FontSize", 10, "FontWeight", "bold", "Color", mosColor);
        end
    end

    yyaxis right;
    offset = max(maxLatency * 0.035, 1);
    for i = 1:numel(x)
        if isfinite(latency(i))
            text(x(i), latency(i) + offset, sprintf("%.0f ms", latency(i)), ...
                "HorizontalAlignment", "center", "VerticalAlignment", "bottom", ...
                "FontSize", 10, "FontWeight", "bold", "Color", latencyColor);
        end
    end
end

function exportFigure(fig, outputDir, baseName)
    pngPath = fullfile(outputDir, baseName + ".png");
    pdfPath = fullfile(outputDir, baseName + ".pdf");
    figPath = fullfile(outputDir, baseName + ".fig");
    exportgraphics(fig, pngPath, "Resolution", 300);
    exportgraphics(fig, pdfPath, "ContentType", "vector");
    savefig(fig, figPath);
end

function applyPaperAxes(ax)
    set(ax, "FontName", "Arial", "FontSize", 12, "LineWidth", 1.1);
    try
        ax.Toolbar.Visible = "off";
    catch
    end
    try
        disableDefaultInteractivity(ax);
    catch
    end
    ax.Title.FontSize = 16;
    ax.Title.FontWeight = "bold";
    ax.XLabel.FontSize = 13;
    ax.YLabel.FontSize = 13;
end

function printCleaningReport(runs)
    fprintf("Measurement cleaning report\n");
    fprintf("---------------------------\n");
    for k = 1:numel(runs)
        fprintf("%s\n", runs(k).label);
        fprintf("  File: %s\n", runs(k).file);
        fprintf("  Time Series kept: %d / %d rows\n", runs(k).cleanTimeSeriesRows, runs(k).rawTimeSeriesRows);
        fprintf("  Client Summary kept: %d / %d clients\n", runs(k).cleanClientRows, runs(k).rawClientRows);
        fprintf("  Media mode: %s\n", runs(k).mediaMode);
        fprintf("  Figure 1 source: %s\n", runs(k).jitterSource);
        fprintf("  Figure 2 source: %s\n", runs(k).qoeSource);
        fprintf("  Avg MOS: %.3f | P95 playout latency: %.3f ms\n", runs(k).avgMos, runs(k).p95LatencyMs);
    end
end

function label = makeScenarioLabel(filePath, mediaMode)
    [~, stem] = fileparts(filePath);
    raw = string(regexprep(stem, "[-_]+", " "));
    raw = strtrim(regexprep(raw, "\s+", " "));

    if strcmpi(raw, "web intercom metrics") || strcmpi(raw, "metrics")
        if strlength(mediaMode) > 0 && ~strcmpi(mediaMode, "unknown")
            label = upper(extractBefore(mediaMode + " ", 2)) + extractAfter(mediaMode, 1) + " measurement";
        else
            label = "Measured run";
        end
    else
        label = raw;
    end
end

function mediaMode = inferMediaMode(cleanClients, rawClients)
    modes = coltext(cleanClients, "media_mode");
    if isempty(modes)
        modes = coltext(rawClients, "media_mode");
    end
    modes = lower(strtrim(modes));
    modes = modes(strlength(modes) > 0);
    if isempty(modes)
        mediaMode = "unknown";
        return;
    end
    uniqueModes = unique(modes, "stable");
    mediaMode = strjoin(uniqueModes, " + ");
end

function value = summaryMetric(T, metricName)
    value = NaN;
    if isempty(T) || ~hasVar(T, "Metric") || ~hasVar(T, "Value")
        return;
    end
    metrics = string(T.("Metric"));
    idx = find(strcmpi(strtrim(metrics), metricName), 1, "first");
    if isempty(idx)
        return;
    end
    values = colnum(T, "Value");
    value = values(idx);
end

function [x, y] = empiricalCdf(values)
    values = sort(values(isfinite(values) & values >= 0));
    n = numel(values);
    if n == 0
        x = [];
        y = [];
        return;
    end
    x = values(:);
    y = (1:n)' ./ n .* 100;
end

function y = percentileValue(values, pct)
    values = sort(values(isfinite(values)));
    n = numel(values);
    if n == 0
        y = NaN;
        return;
    end
    if n == 1
        y = values(1);
        return;
    end
    pos = 1 + (n - 1) * pct / 100;
    lo = floor(pos);
    hi = ceil(pos);
    if lo == hi
        y = values(lo);
    else
        y = values(lo) + (values(hi) - values(lo)) * (pos - lo);
    end
end

function y = meanFinite(values)
    values = values(isfinite(values));
    if isempty(values)
        y = NaN;
    else
        y = mean(values);
    end
end

function x = colnum(T, name)
    if isempty(T) || ~hasVar(T, name)
        x = nan(height(T), 1);
        return;
    end
    x = toNumeric(T.(name));
end

function s = coltext(T, name)
    if isempty(T) || ~hasVar(T, name)
        s = strings(0, 1);
        return;
    end
    value = T.(name);
    if iscell(value)
        s = strings(numel(value), 1);
        for i = 1:numel(value)
            s(i) = string(value{i});
        end
    else
        s = string(value);
    end
    s = s(:);
end

function tf = hasVar(T, name)
    tf = ~isempty(T) && any(strcmp(T.Properties.VariableNames, name));
end

function x = toNumeric(value)
    if isnumeric(value) || islogical(value)
        x = double(value);
    elseif iscell(value)
        x = nan(numel(value), 1);
        for i = 1:numel(value)
            x(i) = scalarToDouble(value{i});
        end
    else
        x = str2double(string(value));
    end
    x = x(:);
end

function x = scalarToDouble(value)
    if isempty(value)
        x = NaN;
    elseif isnumeric(value) || islogical(value)
        x = double(value);
    else
        x = str2double(string(value));
    end
end

function stem = safeStem(stem)
    stem = string(regexprep(stem, "[^A-Za-z0-9_-]", "_"));
end

function y = maxOrZero(values)
    values = values(isfinite(values));
    if isempty(values)
        y = 0;
    else
        y = max(values);
    end
end
