import { asNumberIfValid, upperCaseSymbols } from '../handy'
import { BookChange, OptionSummary, Trade } from '../types'
import { Mapper } from './mapper'

// https://binance-docs.github.io/apidocs/voptions/en/#websocket-market-streams

export class BinanceEuropeanOptionsTradesMapper implements Mapper<'binance-european-options', Trade> {
  canHandle(message: BinanceResponse<any>) {
    if (message.stream === undefined) {
      return false
    }

    return message.stream.endsWith('@trade')
  }

  getFilters(symbols?: string[]) {
    symbols = upperCaseSymbols(symbols)

    return [
      {
        channel: 'trade',
        symbols
      } as const
    ]
  }

  *map(binanceTradeResponse: BinanceResponse<BinanceOptionsTradeData>, localTimestamp: Date) {
    const trade: Trade = {
      type: 'trade',
      symbol: binanceTradeResponse.data.s,
      exchange: 'binance-european-options',
      id: binanceTradeResponse.data.t,
      price: Number(binanceTradeResponse.data.p),
      amount: Number(binanceTradeResponse.data.q),
      side: binanceTradeResponse.data.S === '-1' ? 'sell' : 'buy',
      timestamp: new Date(binanceTradeResponse.data.T),
      localTimestamp: localTimestamp
    }

    yield trade
  }
}

export class BinanceEuropeanOptionsBookChangeMapper implements Mapper<'binance-european-options', BookChange> {
  canHandle(message: BinanceResponse<any>) {
    if (message.stream === undefined) {
      return false
    }

    return message.stream.includes('@depth100')
  }

  getFilters(symbols?: string[]) {
    symbols = upperCaseSymbols(symbols)

    return [
      {
        channel: 'depth100',
        symbols
      } as const
    ]
  }

  *map(message: BinanceResponse<BinanceOptionsDepthData>, localTimestamp: Date) {
    const bookChange: BookChange = {
      type: 'book_change',
      symbol: message.data.s,
      exchange: 'binance-european-options',
      isSnapshot: true,
      bids: message.data.b.map(this.mapBookLevel),
      asks: message.data.a.map(this.mapBookLevel),
      timestamp: message.data.E !== undefined ? new Date(message.data.E) : new Date(message.data.T),
      localTimestamp
    }

    yield bookChange
  }

  protected mapBookLevel(level: BinanceBookLevel) {
    const price = Number(level[0])
    const amount = Number(level[1])
    return { price, amount }
  }
}

export class BinanceEuropeanOptionSummaryMapper implements Mapper<'binance-european-options', OptionSummary> {
  private readonly _indexPrices = new Map<string, number>()
  private readonly _openInterests = new Map<string, number>()

  canHandle(message: BinanceResponse<any>) {
    if (message.stream === undefined) {
      return false
    }

    return message.stream.endsWith('@ticker') || message.stream.endsWith('@index') || message.stream.includes('@openInterest')
  }

  getFilters(symbols?: string[]) {
    symbols = upperCaseSymbols(symbols)

    const indexes =
      symbols !== undefined
        ? symbols.map((s) => {
            const symbolParts = s.split('-')
            return `${symbolParts[0]}USDT`
          })
        : undefined

    const underlyings =
      symbols !== undefined
        ? symbols.map((s) => {
            const symbolParts = s.split('-')
            return `${symbolParts[0]}`
          })
        : undefined

    return [
      {
        channel: 'ticker',
        symbols
      } as const,
      {
        channel: 'index',
        symbols: indexes
      } as const,
      {
        channel: 'openInterest',
        symbols: underlyings
      } as const
    ]
  }

  *map(
    message: BinanceResponse<BinanceOptionsTickerData | BinanceOptionsIndexData | BinanceOptionsOpenInterestData[]>,
    localTimestamp: Date
  ) {
    if (message.stream.endsWith('@index')) {
      const lastIndexPrice = Number((message.data as any).p)
      if (lastIndexPrice > 0) {
        this._indexPrices.set((message.data as any).s, lastIndexPrice)
      }
      return
    }

    if (message.stream.includes('@openInterest')) {
      for (let data of message.data as BinanceOptionsOpenInterestData[]) {
        const openInterest = Number(data.o)
        if (openInterest >= 0) {
          this._openInterests.set(data.s, openInterest)
        }
      }

      return
    }

    const optionInfo = message.data as BinanceOptionsTickerData

    const [base, expiryPart, strikePrice, optionType] = optionInfo.s.split('-')

    const expirationDate = new Date(`20${expiryPart.slice(0, 2)}-${expiryPart.slice(2, 4)}-${expiryPart.slice(4, 6)}Z`)
    expirationDate.setUTCHours(8)

    const isPut = optionType === 'P'

    const underlyingIndex = `${base}USDT`

    let bestBidPrice = asNumberIfValid(optionInfo.bo)
    if (bestBidPrice === 0) {
      bestBidPrice = undefined
    }

    let bestBidAmount = asNumberIfValid(optionInfo.bq)
    if (bestBidAmount === 0) {
      bestBidAmount = undefined
    }
    let bestAskPrice = asNumberIfValid(optionInfo.ao)
    if (bestAskPrice === 0) {
      bestAskPrice = undefined
    }

    let bestAskAmount = asNumberIfValid(optionInfo.aq)
    if (bestAskAmount === 0) {
      bestAskAmount = undefined
    }

    let bestBidIV = bestBidPrice !== undefined ? asNumberIfValid(optionInfo.b) : undefined
    if (bestBidIV === -1) {
      bestBidIV = undefined
    }

    let bestAskIV = bestAskPrice !== undefined ? asNumberIfValid(optionInfo.a) : undefined
    if (bestAskIV === -1) {
      bestAskIV = undefined
    }

    const optionSummary: OptionSummary = {
      type: 'option_summary',
      symbol: optionInfo.s,
      exchange: 'binance-european-options',
      optionType: isPut ? 'put' : 'call',
      strikePrice: Number(strikePrice),
      expirationDate,

      bestBidPrice,
      bestBidAmount,
      bestBidIV,

      bestAskPrice,
      bestAskAmount,
      bestAskIV,

      lastPrice: asNumberIfValid(optionInfo.c),

      openInterest: this._openInterests.get(optionInfo.s),

      markPrice: asNumberIfValid(optionInfo.mp),
      markIV: undefined,

      delta: asNumberIfValid(optionInfo.d),
      gamma: asNumberIfValid(optionInfo.g),
      vega: asNumberIfValid(optionInfo.v),
      theta: asNumberIfValid(optionInfo.t),
      rho: undefined,

      underlyingPrice: this._indexPrices.get(underlyingIndex),
      underlyingIndex,

      timestamp: new Date(optionInfo.E),
      localTimestamp: localTimestamp
    }

    yield optionSummary
  }
}

type BinanceResponse<T> = {
  stream: string
  data: T
}

type BinanceOptionsTradeData = {
  e: 'trade'
  E: 1696118408137
  s: 'DOGE-231006-0.06-C'
  t: '15'
  p: '2.64'
  q: '0.01'
  b: '4647850284614262784'
  a: '4719907951072796672'
  T: 1696118408134
  S: '-1'
}

type BinanceOptionsDepthData = {
  e: 'depth'
  E: 1696118400038
  T: 1696118399082
  s: 'BTC-231027-34000-C'
  u: 1925729
  pu: 1925729
  b: [['60', '7.31'], ['55', '2.5'], ['50', '15'], ['45', '15'], ['40', '34.04']]
  a: [['65', '8.28'], ['70', '38.88'], ['75', '15'], ['1200', '0.01'], ['4660', '0.42']]
}

type BinanceOptionsTickerData = {
  e: '24hrTicker'
  E: 1696118400043
  T: 1696118400000
  s: 'BNB-231013-200-P'
  o: '1'
  h: '1'
  l: '0.9'
  c: '0.9'
  V: '11.08'
  A: '9.97'
  P: '-0.1'
  p: '-0.1'
  Q: '11'
  F: '0'
  L: '8'
  n: 1
  bo: '1'
  ao: '1.7'
  bq: '50'
  aq: '50'
  b: '0.35929501'
  a: '0.43317497'
  d: '-0.16872899'
  t: '-0.16779034'
  g: '0.0153237'
  v: '0.09935076'
  vo: '0.41658748'
  mp: '1.5'
  hl: '37.1'
  ll: '0.1'
  eep: '0'
}

type BinanceOptionsIndexData = { e: 'index'; E: 1696118400040; s: 'BNBUSDT'; p: '214.6133998' }

type BinanceOptionsOpenInterestData = { e: 'openInterest'; E: 1696118400042; s: 'XRP-231006-0.46-P'; o: '39480.0'; h: '20326.64319' }

type BinanceBookLevel = [string, string]
