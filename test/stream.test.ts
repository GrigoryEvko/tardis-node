import {
  compute,
  computeBookSnapshots,
  computeTradeBars,
  Exchange,
  EXCHANGES,
  getExchangeDetails,
  normalizeBookChanges,
  normalizeDerivativeTickers,
  normalizeLiquidations,
  normalizeTrades,
  streamNormalized,
  normalizeBookTickers
} from '../dist'

const exchangesWithDerivativeInfo: Exchange[] = [
  'bitmex',
  'binance-futures',
  'bitfinex-derivatives',
  'cryptofacilities',
  'deribit',
  'okex-futures',
  'okex-swap',
  'bybit',
  'phemex',
  'ftx',
  'delta',
  'binance-delivery',
  'huobi-dm',
  'huobi-dm-swap',
  'gate-io-futures',
  'coinflex',
  'huobi-dm-linear-swap'
]

const exchangesWithLiquidationsSupport: Exchange[] = [
  'ftx',
  'bitmex',
  'deribit',
  'binance-futures',
  'binance-delivery',
  'bitfinex-derivatives',
  'cryptofacilities',
  'huobi-dm',
  'huobi-dm-swap'
]

const exchangesWithBookTickerInfo: Exchange[] = [
  'ascendex',
  'binance',
  'binance-futures',
  'binance-delivery',
  'binance-dex',
  'bitfinex',
  'bitfinex-derivatives',
  'bitflyer',
  'bitmex',
  'coinbase',
  'cryptofacilities',
  'deribit',
  'ftx',
  'ftx-us',
  'huobi',
  'huobi-dm',
  'huobi-dm-linear-swap',
  'huobi-dm-swap',
  'kraken',
  'okex',
  'okex-futures',
  'okex-swap',
  'okex-options',
  'okcoin',
  'serum'
]

describe('exchange-details', () => {
  test('provides exchange info', async () => {
    const exchange = 'binance'
    const exchangeDetails = await getExchangeDetails(exchange)

    expect(exchangeDetails.availableChannels).toEqual([
      'trade',
      'aggTrade',
      'ticker',
      'depth',
      'depthSnapshot',
      'bookTicker',
      'recentTrades',
      'borrowInterest',
      'tokenNav'
    ])
  })
})

describe('stream', () => {
  test(
    'streams normalized real-time messages for each supported exchange',
    async () => {
      const exchanges: Exchange[] = []
      await Promise.all(
        EXCHANGES.map(async (exchange) => {
          if (
            exchange === 'binance-dex' ||
            exchange === 'binance-jersey' ||
            exchange === 'huobi-dm-options' ||
            exchange === 'star-atlas' ||
            exchange === 'binance-options' ||
            exchange === 'okex-futures' ||
            exchange === 'okex-options' ||
            exchange === 'delta' ||
            exchange === 'kucoin' ||
            exchange === 'phemex' ||
            exchange === 'bybit-spot' ||
            exchange === 'coinbase' ||
            exchange === 'crypto-com-derivatives' ||
            exchange === 'mango' ||
            exchange === 'serum' ||
            exchange === 'ftx' ||
            exchange === 'ftx-us' ||
            exchange === 'binance' ||
            exchange === 'binance-futures' ||
            exchange === 'binance-delivery' ||
            exchange === 'bitnomial' ||
            exchange === 'bybit' ||
            exchange === 'bybit-options' ||
            exchange === 'gemini' ||
            exchange === 'coinflex' ||
            exchange === 'huobi' ||
            exchange === 'gate-io' ||
            exchange === 'ascendex' ||
            exchange === 'binance-european-options'
          ) {
            return
          }

          exchanges.push(exchange)

          const exchangeDetails = await getExchangeDetails(exchange)
          const normalizers: any[] = [normalizeTrades, normalizeBookChanges]

          if (exchangesWithDerivativeInfo.includes(exchange)) {
            normalizers.push(normalizeDerivativeTickers)
          }

          if (exchangesWithLiquidationsSupport.includes(exchange)) {
            normalizers.push(normalizeLiquidations)
          }

          if (exchangesWithBookTickerInfo.includes(exchange)) {
            normalizers.push(normalizeBookTickers)
          }

          var symbols = exchangeDetails.availableSymbols
            .filter((s) => s.availableTo === undefined || new Date(s.availableTo).valueOf() > new Date().valueOf())
            .slice(0, 10)
            .map((s) => s.id)

          const messages = streamNormalized(
            {
              exchange,
              symbols,
              withDisconnectMessages: true,
              timeoutIntervalMS: 30 * 1000,
              onError: (err) => {
                console.log('Error', exchange, err)
              }
            },
            ...normalizers
          )

          const messagesWithComputables = compute(
            messages,
            computeTradeBars({ interval: 10, kind: 'time' }),
            computeBookSnapshots({ interval: 0, depth: 3 })
          )

          let count = 0
          let snapshots = 0

          for await (const msg of messagesWithComputables) {
            // reset counters if we've received disconnect
            if (msg.type === 'disconnect') {
              count = 0
              snapshots = 0
            }

            if (msg.type === 'book_change' && (msg as any).isSnapshot) {
              snapshots++
            }

            if (snapshots >= symbols.length - 1) {
              count++
              if (count >= 10) {
                break
              }
            }
          }
          exchanges.splice(exchanges.indexOf(exchange), 1)
          console.log('remaining', exchanges)
        })
      )
    },
    1000 * 60 * 2
  )
})
