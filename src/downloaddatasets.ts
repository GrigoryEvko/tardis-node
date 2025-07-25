import { existsSync, ensureDirSync } from 'fs-extra'
import pMap from 'p-map'
import { debug } from './debug'
import { DatasetType } from './exchangedetails'
import { addDays, doubleDigit, download, parseAsUTCDate, sequence } from './handy'
import { getOptions } from './options'
import { Exchange } from './types'

const DEFAULT_CONCURRENCY_LIMIT = 20
const MILLISECONDS_IN_SINGLE_DAY = 24 * 60 * 60 * 1000
const DEFAULT_DOWNLOAD_DIR = './datasets'

const options = getOptions()

export async function downloadDatasets(downloadDatasetsOptions: DownloadDatasetsOptions): Promise<DownloadSummary> {
    const { exchange, dataTypes, from, to, symbols } = downloadDatasetsOptions
    const apiKey = downloadDatasetsOptions.apiKey !== undefined ? downloadDatasetsOptions.apiKey : options.apiKey
    const downloadDir = downloadDatasetsOptions.downloadDir !== undefined ? downloadDatasetsOptions.downloadDir : DEFAULT_DOWNLOAD_DIR
    const format = downloadDatasetsOptions.format !== undefined ? downloadDatasetsOptions.format : 'csv'
    const getFilename = downloadDatasetsOptions.getFilename !== undefined ? downloadDatasetsOptions.getFilename : getFilenameDefault
    const skipIfExists = downloadDatasetsOptions.skipIfExists === undefined ? true : downloadDatasetsOptions.skipIfExists
    const concurrencyLimit = downloadDatasetsOptions.concurrencyLimit !== undefined ? downloadDatasetsOptions.concurrencyLimit : DEFAULT_CONCURRENCY_LIMIT
    const flattenConcurrency = downloadDatasetsOptions.flattenConcurrency === true
    const onProgress = downloadDatasetsOptions.onProgress
    const continueOnError = downloadDatasetsOptions.continueOnError === true

    // in case someone provided 'api/exchange' symbol, transform it to symbol that is accepted by datasets API
    const datasetsSymbols = symbols.map((s) => s.replace(/\/|:/g, '-').toUpperCase())

    if (flattenConcurrency) {
        // New flattened concurrency model for high performance
        return downloadDatasetsFullyAsync({
            ...downloadDatasetsOptions,
            symbols: datasetsSymbols,
            apiKey,
            downloadDir,
            format,
                getFilename,
                skipIfExists,
                concurrencyLimit,
                onProgress,
                continueOnError
        })
    }

    // Original nested loop implementation for backward compatibility - now with summary return
    const startTimestamp = new Date().valueOf()
    let totalCompleted = 0
    let totalSkipped = 0
    let totalErrors = 0
    const errorDetails: Array<{ symbol: string; dataType: DatasetType; error: Error }> = []

    for (const symbol of datasetsSymbols) {
        for (const dataType of dataTypes) {
            const { daysCountToFetch, startDate } = getDownloadDateRange(downloadDatasetsOptions)
            debug('dataset download started for %s %s %s from %s to %s', exchange, dataType, symbol, from, to)

            try {
                if (daysCountToFetch > 1) {
                    // start with downloading last day of the range, validates is API key has access to the end range of requested data
                    const lastDayResult = await downloadDataSet(
                        getDownloadOptions({
                            exchange,
                            symbol,
                            apiKey,
                            downloadDir,
                            dataType,
                            format,
                                getFilename,
                                date: addDays(startDate, daysCountToFetch - 1)
                        }),
                        skipIfExists
                    )
                    if (lastDayResult?.skipped) totalSkipped++
                        else if (lastDayResult?.downloaded) totalCompleted++
                }

                // then download the first day of the range, validates is API key has access to the start range of requested data
                const firstDayResult = await downloadDataSet(
                    getDownloadOptions({
                        exchange,
                        symbol,
                        apiKey,
                        downloadDir,
                        dataType,
                        format,
                            getFilename,
                            date: startDate
                    }),
                    skipIfExists
                )
                if (firstDayResult?.skipped) totalSkipped++
                    else if (firstDayResult?.downloaded) totalCompleted++

                        // download the rest concurrently up to the CONCURRENCY_LIMIT
                        const remainingResults = await pMap(
                            sequence(daysCountToFetch - 1, 1),
                                                            async (offset) => {
                                                                const result = await downloadDataSet(
                                                                    getDownloadOptions({
                                                                        exchange,
                                                                        symbol,
                                                                        apiKey,
                                                                        downloadDir,
                                                                        dataType,
                                                                        format,
                                                                            getFilename,
                                                                            date: addDays(startDate, offset)
                                                                    }),
                                                                    skipIfExists
                                                                )
                                                                return result
                                                            },
                                                            { concurrency: concurrencyLimit }
                        )

                        // Count results
                        for (const result of remainingResults) {
                            if (result?.skipped) totalSkipped++
                                else if (result?.downloaded) totalCompleted++
                        }

                        const elapsedSeconds = (new Date().valueOf() - startTimestamp) / 1000
                        debug('dataset download finished for %s %s %s from %s to %s, time: %s seconds', exchange, dataType, symbol, from, to, elapsedSeconds)

            } catch (error: any) {
                totalErrors++
                errorDetails.push({ symbol, dataType, error })
                debug('error downloading %s %s %s: %o', exchange, dataType, symbol, error)

                if (!continueOnError) {
                    throw error
                }
            }
        }
    }

    const elapsedSeconds = (new Date().valueOf() - startTimestamp) / 1000
    return {
        exchange,
        from,
        to,
        totalTasks: totalCompleted + totalSkipped + totalErrors,
        completed: totalCompleted,
        skipped: totalSkipped,
        errors: totalErrors,
        elapsedSeconds,
        errorDetails: errorDetails.length > 0 ? errorDetails : undefined
    }
}

// Fully async, non-blocking implementation
async function downloadDatasetsFullyAsync(options: {
    exchange: Exchange
    dataTypes: DatasetType[]
    symbols: string[]
    from: string
    to: string
    apiKey: string
    downloadDir: string
    format: string
        getFilename: (options: GetFilenameOptions) => string
        skipIfExists: boolean
        concurrencyLimit: number
        onProgress?: (progress: DownloadProgress) => void
        continueOnError: boolean
}): Promise<DownloadSummary> {
    const { exchange, dataTypes, symbols, apiKey, downloadDir, format, getFilename, skipIfExists, concurrencyLimit, onProgress, continueOnError } = options
    const { daysCountToFetch, startDate } = getDownloadDateRange(options)

    const startTimestamp = new Date().valueOf()
    debug('fully async dataset download started for %s from %s to %s', exchange, options.from, options.to)

    // Generate all download tasks as a flat array
    const downloadTasks = generateDownloadTasks({
        symbols,
        dataTypes,
        startDate,
        daysCountToFetch
    })

    debug('generated %d download tasks', downloadTasks.length)

    // Track progress
    let completed = 0
    let skipped = 0
    let errors = 0
    const totalTasks = downloadTasks.length
    const errorDetails: Array<{ task: DownloadTask; error: Error }> = []

    // Process all tasks with full parallelism
    await pMap(
        downloadTasks,
        async (task) => {
            const downloadOptions = getDownloadOptions({
                exchange,
                symbol: task.symbol,
                apiKey,
                downloadDir,
                dataType: task.dataType,
                format,
                    getFilename,
                    date: task.date
            })

            try {
                if (skipIfExists && existsSync(downloadOptions.downloadPath)) {
                    skipped++
                    debug('dataset %s already exists, skipping download', downloadOptions.downloadPath)

                    if (onProgress) {
                        onProgress({
                            completed: completed + skipped,
                            total: totalTasks,
                            errors,
                            currentFile: downloadOptions.downloadPath,
                            status: 'skipped'
                        })
                    }
                    return { success: true, skipped: true }
                }

                // Ensure directory exists
                const dir = downloadOptions.downloadPath.substring(0, downloadOptions.downloadPath.lastIndexOf('/'))
                ensureDirSync(dir)

                // Download with automatic .unconfirmed handling (already in download function)
                await download(downloadOptions)

                completed++

                if (onProgress) {
                    onProgress({
                        completed: completed + skipped,
                        total: totalTasks,
                        errors,
                        currentFile: downloadOptions.downloadPath,
                        status: 'downloaded'
                    })
                }

                return { success: true, skipped: false }
            } catch (error: any) {
                errors++
                errorDetails.push({ task, error })

                debug('download error for %s: %o', downloadOptions.downloadPath, error)

                if (onProgress) {
                    onProgress({
                        completed: completed + skipped,
                        total: totalTasks,
                        errors,
                        currentFile: downloadOptions.downloadPath,
                        status: 'error',
                        error: error.message
                    })
                }

                if (!continueOnError) {
                    throw error
                }

                return { success: false, error }
            }
        },
        {
            concurrency: concurrencyLimit,
            stopOnError: !continueOnError
        }
    )

    const elapsedSeconds = (new Date().valueOf() - startTimestamp) / 1000

    const summary: DownloadSummary = {
        exchange,
        from: options.from,
        to: options.to,
        totalTasks,
        completed,
        skipped,
        errors,
        elapsedSeconds,
        errorDetails: errorDetails.length > 0 ? errorDetails : undefined
    }

    debug('fully async dataset download finished: %o', summary)

    if (errors > 0 && !continueOnError) {
        throw new Error(`Download failed with ${errors} errors. First error: ${errorDetails[0]?.error.message}`)
    }

    return summary
}

// Generate all download tasks without blocking
function generateDownloadTasks(options: {
    symbols: string[]
    dataTypes: DatasetType[]
    startDate: Date
    daysCountToFetch: number
}): DownloadTask[] {
    const { symbols, dataTypes, startDate, daysCountToFetch } = options
    const tasks: DownloadTask[] = []

    for (const symbol of symbols) {
        for (const dataType of dataTypes) {
            for (let dayOffset = 0; dayOffset < daysCountToFetch; dayOffset++) {
                tasks.push({
                    symbol,
                    dataType,
                    date: addDays(startDate, dayOffset),
                           dayOffset,
                           isFirst: dayOffset === 0,
                           isLast: dayOffset === daysCountToFetch - 1
                })
            }
        }
    }

    return tasks
}

async function downloadDataSet(downloadOptions: DownloadOptions, skipIfExists: boolean): Promise<{ downloaded?: boolean; skipped?: boolean }> {
    if (skipIfExists && existsSync(downloadOptions.downloadPath)) {
        debug('dataset %s already exists, skipping download', downloadOptions.downloadPath)
        return { skipped: true }
    } else {
        // Ensure directory exists before download
        const dir = downloadOptions.downloadPath.substring(0, downloadOptions.downloadPath.lastIndexOf('/'))
        ensureDirSync(dir)

        await download(downloadOptions)
        return { downloaded: true }
    }
}

function getDownloadOptions({
    apiKey,
    exchange,
    dataType,
    date,
    symbol,
    format,
        downloadDir,
        getFilename
}: {
    exchange: Exchange
    dataType: DatasetType
    symbol: string
    date: Date
    format: string
        apiKey: string
        downloadDir: string
        getFilename: (options: GetFilenameOptions) => string
}): DownloadOptions {
    const year = date.getUTCFullYear()
    const month = doubleDigit(date.getUTCMonth() + 1)
    const day = doubleDigit(date.getUTCDate())

    const url = `${options.datasetsEndpoint}/${exchange}/${dataType}/${year}/${month}/${day}/${symbol}.${format}.gz`
    const filename = getFilename({
        dataType,
        date,
        exchange,
        format,
            symbol
    })

    const downloadPath = `${downloadDir}/${filename}`

    return {
        url,
        downloadPath,
        userAgent: options._userAgent,
        apiKey
    }
}

type DownloadOptions = Parameters<typeof download>[0]

function getFilenameDefault({ exchange, dataType, format, date, symbol }: GetFilenameOptions) {
    return `${exchange}_${dataType}_${date.toISOString().split('T')[0]}_${symbol}.${format}.gz`
}

function getDownloadDateRange({ from, to }: Pick<DownloadDatasetsOptions, 'from' | 'to'>) {
    if (!from || isNaN(Date.parse(from))) {
        throw new Error(`Invalid "from" argument: ${from}. Please provide valid date string.`)
    }

    if (!to || isNaN(Date.parse(to))) {
        throw new Error(`Invalid "to" argument: ${to}. Please provide valid date string.`)
    }

    const toDate = parseAsUTCDate(to)
    const fromDate = parseAsUTCDate(from)
    const daysCountToFetch = Math.floor((toDate.getTime() - fromDate.getTime()) / MILLISECONDS_IN_SINGLE_DAY) + 1

    if (daysCountToFetch < 1) {
        throw new Error(`Invalid "to" and "from" arguments combination. Please provide "to" day that is later than "from" day.`)
    }

    return {
        startDate: fromDate,
        daysCountToFetch
    }
}

type GetFilenameOptions = {
    exchange: Exchange
    dataType: DatasetType
    symbol: string
    date: Date
    format: string
}

type DownloadTask = {
    symbol: string
    dataType: DatasetType
    date: Date
    dayOffset: number
    isFirst: boolean
    isLast: boolean
}

type DownloadProgress = {
    completed: number
    total: number
    errors: number
    currentFile: string
    status: 'downloaded' | 'skipped' | 'error'
    error?: string
}

type DownloadSummary = {
    exchange: Exchange
    from: string
    to: string
    totalTasks: number
    completed: number
    skipped: number
    errors: number
    elapsedSeconds: number
    errorDetails?: Array<{ symbol?: string; dataType?: DatasetType; task?: DownloadTask; error: Error }>
}

type DownloadDatasetsOptions = {
    exchange: Exchange
    dataTypes: DatasetType[]
    symbols: string[]
    from: string
    to: string
    format?: 'csv'
        apiKey?: string
        downloadDir?: string
        getFilename?: (options: GetFilenameOptions) => string
        skipIfExists?: boolean
        concurrencyLimit?: number
        flattenConcurrency?: boolean
        onProgress?: (progress: DownloadProgress) => void
        continueOnError?: boolean
}
