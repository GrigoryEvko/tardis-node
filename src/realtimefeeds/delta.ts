import { Filter } from '../types'
import { RealTimeFeedBase } from './realtimefeed'

export class DeltaRealTimeFeed extends RealTimeFeedBase {
  protected readonly wssURL = 'wss://socket.delta.exchange'

  protected mapToSubscribeMessages(filters: Filter<string>[]) {
    return filters.map((filter) => {
      return {
        type: 'subscribe',
        payload: {
          channels: [
            {
              name: filter.channel,
              symbols:
                filter.symbols !== undefined && filter.channel === 'mark_price' ? filter.symbols.map((s) => `MARK:${s}`) : filter.symbols
            }
          ]
        }
      }
    })
  }

  protected messageIsError(message: any): boolean {
    return message.error !== undefined && message.error !== null
  }

  protected sendCustomPing = () => {
    this.send({ type: 'ping' })
  }

  protected messageIsHeartbeat(msg: any) {
    return msg.type === 'pong'
  }
}
