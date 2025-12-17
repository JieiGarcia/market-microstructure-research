import pandas as pd
import numpy as np

class IntraBarZigzag:
    def __init__(self, tick_df: pd.DataFrame, resample_frequency: int) -> None:
        # Setup
        self.df = tick_df.copy()
        self.df_formatted_flag = False
        self.resample_frequency = resample_frequency
        self.resampled_df = None
        self.weekends_excluded_flag = False

        # Candle Info
        self.open_price = None
        self.high_price = None
        self.low_price = None
        self.close_price = None
        self.open_time = None
        self.high_time = None
        self.low_time = None
        self.close_time = None

        # For LH Formation
        self.High_Between_Open_Low_price = None
        self.High_Between_Open_Low_time = None
        self.Low_Between_High_Close_price = None
        self.Low_Between_High_Close_time = None
        # For HL Formation
        self.Low_Between_Open_High_price = None
        self.Low_Between_Open_High_time = None
        self.High_Between_Low_Close_price = None
        self.High_Between_Low_Close_time = None

        # For Candle Type Classification
        self.initial_candle_type = None
        self.refined_candle_type = None

        # For Swing Detection
        self.swings = []
        self.handler_func_name = None
        self.connection_rules = {
            ('+','+'): '_handle_continuation',
            ('-','-'): '_handle_continuation',
            ('+','-'): '_handle_reversal',
            ('-','+'): '_handle_reversal',
            ('+','.'): '_handle_Single_Event',
            ('-','.'): '_handle_Single_Event'
        }
        self.candle_properties = {
            # Bullish LH FullWick
            'Bullish_LH_FullWick': {
                'connectors': ('-','-'),
                'pivots': ('low', 'high')
            },
            'Bullish_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose': {
                'connectors': ('+','+'),
                'pivots': ('high_betweenOpenLow', 'low', 'high', 'low_betweenHighClose')
            },
            'Bullish_LH_FullWick_OpenUpDownToLow': {
                'connectors': ('+','-'),
                'pivots': ('high_betweenOpenLow', 'low', 'high')
            },
            'Bullish_LH_FullWick_PostHighDipBelowClose': {
                'connectors': ('-','+'),
                'pivots': ('low', 'high', 'low_betweenHighClose')
            },
            # Bullish LH TopWick
            'Bullish_LH_TopWick': {
                'connectors': ('+','-'),
                'pivots': ('high',)
            },
            'Bullish_LH_TopWick_PostHighDipBelowClose': {
                'connectors': ('+','+'),
                'pivots': ('high', 'low_betweenHighClose')
            },
            # Bullish LH BottomWick
            'Bullish_LH_BottomWick': {
                'connectors': ('-','+'),
                'pivots': ('low',)
            },
            'Bullish_LH_BottomWick_OpenUpDownToLow': {
                'connectors': ('+','+'),
                'pivots': ('high_betweenOpenLow', 'low')
            },
            # Bullish LH NoWick
            'Bullish_LH_NoWick': {
                'connectors': ('+','+'),
                'pivots': None
            },
            # Bullish HL FullWick
            'Bullish_HL_FullWick': {
                'connectors': ('+','+'),
                'pivots': ('high','low')
            },
            'Bullish_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose': {
                'connectors': ('-','-'),
                'pivots': ('low_betweenOpenHigh','high','low','high_betweenLowClose')
            },
            'Bullish_HL_FullWick_OpenDownThenUpToHigh': {
                'connectors': ('-','+'),
                'pivots': ('low_betweenOpenHigh','high','low')
            },
            'Bullish_HL_FullWick_PostLowRallyAboveClose': {
                'connectors': ('+','-'),
                'pivots': ('high','low','high_betweenLowClose')
            },
            # Bullish HL BottomWick
            'Bullish_HL_BottomWick': {
                'connectors': ('+','+'),
                'pivots': ('high','low')
            },
            'Bullish_HL_BottomWick_OpenDownThenUpToHigh': {
                'connectors': ('-','+'),
                'pivots': ('low_betweenOpenHigh', 'high', 'low')
            },
            # Bearish HL FullWick
            'Bearish_HL_FullWick': {
                'connectors': ('+','+'),
                'pivots': ('high','low')
            },
            'Bearish_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose': {
                'connectors': ('-','-'),
                'pivots': ('low_betweenOpenHigh','high','low','high_betweenLowClose')
            },
            'Bearish_HL_FullWick_OpenDownThenUpToHigh': {
                'connectors': ('-','+'),
                'pivots': ('low_betweenOpenHigh','high','low')
            },
            'Bearish_HL_FullWick_PostLowRallyAboveClose': {
                'connectors': ('+','-'),
                'pivots': ('high','low','high_betweenLowClose')
            },
            # Bearish HL BottomWick
            'Bearish_HL_BottomWick': {
                'connectors': ('-','+'),
                'pivots': ('low',)
            },
            'Bearish_HL_BottomWick_PostLowRallyAboveClose': {
                'connectors': ('-','-'),
                'pivots': ('low','high_betweenLowClose')
            },
            # Bearish HL TopWick
            'Bearish_HL_TopWick': {
                'connectors': ('+','-'),
                'pivots': ('high',)
            },
            'Bearish_HL_TopWick_OpenDownThenUpToHigh': {
                'connectors': ('-','-'),
                'pivots': ('low_betweenOpenHigh','high')
            },
            # Bearish HL NoWick
            'Bearish_HL_NoWick': {
                'connectors': ('-','-'),
                'pivots': None
            },
            # Bearish LH FullWick
            'Bearish_LH_FullWick': {
                'connectors': ('-','-'),
                'pivots': ('low','high')
            },
            'Bearish_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose': {
                'connectors': ('+','+'),
                'pivots': ('high_betweenOpenLow','low','high','low_betweenHighClose')
            },
            'Bearish_LH_FullWick_OpenUpDownToLow': {
                'connectors': ('+','-'),
                'pivots': ('high_betweenOpenLow','low','high')
            },
            'Bearish_LH_FullWick_PostHighDipBelowClose': {
                'connectors': ('-','+'),
                'pivots': ('low','high','low_betweenHighClose')
            },
            # Bearish LH TopWick
            'Bearish_LH_TopWick': {
                'connectors': ('-','-'),
                'pivots': ('low','high')
            },
            'Bearish_LH_TopWick_OpenUpDownToLow': {
                'connectors': ('+','-'),
                'pivots': ('high_betweenOpenLow','low','high')
            },
            # Doji LH
            'Doji_LH_FullWick': {
                'connectors': ('-','-'),
                'pivots': ('low','high')
            },
            'Doji_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose': {
                'connectors': ('+','+'),
                'pivots': ('high_betweenOpenLow','low','high','low_betweenHighClose')
            },
            'Doji_LH_FullWick_OpenUpDownToLow': {
                'connectors': ('+','-'),
                'pivots': ('high_betweenOpenLow','low','high')
            },
            'Doji_LH_FullWick_PostHighDipBelowClose': {
                'connectors': ('-','+'),
                'pivots': ('low','high','low_betweenHighClose')
            },
            # Doji LH TopWick
            'Doji_LH_TopWick': {
                'connectors': ('+','-'),
                'pivots': ('high',)
            },
            # Doji HL FullWick
            'Doji_HL_FullWick': {
                'connectors': ('+','+'),
                'pivots': ('high','low')
            },
            'Doji_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose': {
                'connectors': ('-','-'),
                'pivots': ('low_betweenOpenHigh','high','low','high_betweenLowClose')
            },
            'Doji_HL_FullWick_OpenDownThenUpToHigh': {
                'connectors': ('-','+'),
                'pivots': ('low_betweenOpenHigh','high','low')
            },
            'Doji_HL_FullWick_PostLowRallyAboveClose': {
                'connectors': ('+','-'),
                'pivots': ('high','low','high_betweenLowClose')
            },
            # Doji HL BottomWick
            'Doji_HL_BottomWick': {
                'connectors': ('-','+'),
                'pivots': ('low',)
            },
            # Single Event
            'Single_Event': {
                'connectors': ('.'),
                'pivots': None
            }
        }

    def runSetup(self) -> None:
        """Format Dataframe"""
        self.format_dataframe()

        """Resample The Dataframe"""
        self.resampled_df = self.df.resample(str(self.resample_frequency) + 'min').apply(self._aggregate_ticks_with_timestamps)
        print('resampling completed')

        """Exclude Weekends"""
        self._exclude_weekends()
        print('setup completed')

    def runDetection(self):
        if self.weekends_excluded_flag:
            # Initialize
            self.candle1 = None
            self.candle2 = None
            self.connection = None

            for index, row in self.resampled_df.iterrows():
                if row['candle_type'] == 'blank':
                    print('skipping a blank candle')
                    continue
                if self.candle1 is None:
                    if row['candle_type'] == 'Single_Event':
                        print('Skipping a Single_event candle for the first candle1')
                        continue
                    self.candle1 = {
                        'open_price': row['open_price'],
                        'high_price': row['high_price'],
                        'low_price': row['low_price'],
                        'close_price': row['close_price'],
                        'open_time': row['open_time'],
                        'high_time': row['high_time'],
                        'low_time': row['low_time'],
                        'close_time': row['close_time'],
                        'candle_type': row['candle_type'],
                        # For LH Formations
                        'high_between_open_low_price': row['high_between_open_low_price'],
                        'high_between_open_low_time': row['high_between_open_low_time'],
                        'low_between_high_close_price': row['low_between_high_close_price'],
                        'low_between_high_close_time': row['low_between_high_close_time'],
                        # For HL Formations
                        'low_between_open_high_price': row['low_between_open_high_price'],
                        'low_between_open_high_time': row['low_between_open_high_time'],
                        'high_between_low_close_price': row['high_between_low_close_price'],
                        'high_between_low_close_time': row['high_between_low_close_time'],
                        # Connectors & Pivots
                        'connectors': self.candle_properties[row['candle_type']]['connectors'],
                        'pivots': self.candle_properties[row['candle_type']]['pivots'],
                    }
                    continue

                self.candle2 = {
                    'open_price': row['open_price'],
                    'high_price': row['high_price'],
                    'low_price': row['low_price'],
                    'close_price': row['close_price'],
                    'open_time': row['open_time'],
                    'high_time': row['high_time'],
                    'low_time': row['low_time'],
                    'close_time': row['close_time'],
                    'candle_type': row['candle_type'],
                    # For LH Formations
                    'high_between_open_low_price': row['high_between_open_low_price'],
                    'high_between_open_low_time': row['high_between_open_low_time'],
                    'low_between_high_close_price': row['low_between_high_close_price'],
                    'low_between_high_close_time': row['low_between_high_close_time'],
                    # For HL Formations
                    'low_between_open_high_price': row['low_between_open_high_price'],
                    'low_between_open_high_time': row['low_between_open_high_time'],
                    'high_between_low_close_price': row['high_between_low_close_price'],
                    'high_between_low_close_time': row['high_between_low_close_time'],
                    # Connectors & Pivots
                    'connectors': self.candle_properties[row['candle_type']]['connectors'],
                    'pivots': self.candle_properties[row['candle_type']]['pivots'],
                }

                # Connect the back connector of Candle1 to the front connector of Candle2
                self.connection = (self.candle1['connectors'][1], self.candle2['connectors'][0])

                # Call handler function
                self.handler_func_name = getattr(self, self.connection_rules[self.connection])
                if self.handler_func_name:
                    self.handler_func_name()

                # Overwrite candle1 with candle2
                self.candle1 = self.candle2
        else:
            print('Complete the .setup() first!')
            return None

    def format_dataframe(self):
        if not self.df_formatted_flag:
            self.df.drop(columns=["Ask", "AskVolume", "BidVolume"],inplace=True)
            self.df.rename(columns={"Time (EET)": "time", "Bid": "price"}, inplace=True)
            self.df.set_index("time", inplace=True)
            self.df.index = pd.to_datetime(self.df.index)
            print("Dataframe is now formatted!")
            self.df_formatted_flag = True
        else:
            print("df already formatted, moving to the next procedure!")

    def _aggregate_ticks_with_timestamps(self, group):
        if group.empty:
            # Return NaNs if there are no ticks in the minute
            return pd.Series({
                'open_price': np.nan,
                'high_price': np.nan,
                'low_price': np.nan,
                'close_price': np.nan,
                'open_time': pd.NaT,
                'high_time': pd.NaT,
                'low_time': pd.NaT,
                'close_time': pd.NaT,
                'candle_type': 'blank',
                # For LH Formations
                'high_between_open_low_price': np.nan,
                'high_between_open_low_time': pd.NaT,
                'low_between_high_close_price': np.nan,
                'low_between_high_close_time': pd.NaT,
                # For HL Formations
                'low_between_open_high_price': np.nan,
                'low_between_open_high_time': pd.NaT,
                'high_between_low_close_price': np.nan,
                'high_between_low_close_time': pd.NaT
            })

        # Update Candle data
        self.open_price = group['price'].iloc[0]
        self.high_price = group['price'].max()
        self.low_price = group['price'].min()
        self.close_price = group['price'].iloc[-1]
        self.open_time = group.index[0]
        self.high_time = group['price'].idxmax()
        self.low_time = group['price'].idxmin()
        self.close_time = group.index[-1]

        # Candle Type classification
        self.initial_candle_type = self._initial_candle_classification()
        self.refined_candle_type = self._refined_candle_classification(candle_type=self.initial_candle_type, group=group)

        result_series = pd.Series({
            'open_price': group['price'].iloc[0],
            'high_price': group['price'].max(),
            'low_price': group['price'].min(),
            'close_price': group['price'].iloc[-1],
            'open_time': self.open_time,
            'high_time': self.high_time,
            'low_time': self.low_time,
            'close_time': self.close_time,
            'candle_type': self.refined_candle_type,
            # For LH Formations
            'high_between_open_low_price': self.High_Between_Open_Low_price,
            'high_between_open_low_time': self.High_Between_Open_Low_time,
            'low_between_high_close_price': self.Low_Between_High_Close_price,
            'low_between_high_close_time': self.Low_Between_High_Close_time,
            # For HL Formations
            'low_between_open_high_price': self.Low_Between_Open_High_price,
            'low_between_open_high_time': self.Low_Between_Open_High_time,
            'high_between_low_close_price': self.High_Between_Low_Close_price,
            'high_between_low_close_time': self.High_Between_Low_Close_time
        })

        print(f"Returning Series for group starting at {group.index[0]}:\n{result_series}\n") # Add this line

        return result_series


    def _initial_candle_classification(self) -> str:
        # Bullish
        if self.close_price > self.open_price:
            # LH
            if self.low_time < self.high_time:
                # Full Wicks
                if self.high_price > self.close_price and self.low_price < self.open_price:
                    return 'Bullish_LH_FullWick'
                # Top Wick
                elif self.high_price > self.close_price and self.low_price == self.open_price:
                    return 'Bullish_LH_TopWick'
                # Bottom Wick
                elif self.high_price == self.close_price and self.low_price < self.open_price:
                    return 'Bullish_LH_BottomWick'
                # No Wicks
                elif self.high_price == self.close_price and self.low_price == self.open_price:
                    return 'Bullish_LH_NoWick'
                # Unknown
                else:
                    return '__bullish_LH_UNKNOWN'
            # HL
            elif self.high_time < self.low_time:
                # Full Wicks
                if self.high_price > self.close_price and self.low_price < self.open_price:
                    return 'Bullish_HL_FullWick'
                # Bottom Wicks
                elif self.high_price == self.close_price and self.low_price < self.open_price:
                    return 'Bullish_HL_BottomWick'
                # Unknown
                else:
                    return '__bullish_HL_UNKNOWN'
            # Unknown
            else:
                return '__bullish_UNKNOWN'
        # Bearish
        elif self.close_price < self.open_price:
            # HL
            if self.high_time < self.low_time:
                # Full Wicks
                if self.high_price > self.open_price and self.low_price < self.close_price:
                    return 'Bearish_HL_FullWick'
                # Bottom Wick
                elif self.high_price == self.open_price and self.low_price < self.close_price:
                    return 'Bearish_HL_BottomWick'
                # Top Wick
                elif self.high_price > self.open_price and self.low_price == self.close_price:
                    return 'Bearish_HL_TopWick'
                # No Wicks
                elif self.high_price == self.open_price and self.low_price == self.close_price:
                    return 'Bearish_HL_NoWick'
                # Unknown
                else:
                    return '__bearish_HL_UNKNOWN'
            # LH
            elif self.low_time < self.high_time:
                # Full Wicks
                if self.high_price > self.open_price and self.low_price < self.close_price:
                    return 'Bearish_LH_FullWick'
                # Top Wick
                elif self.high_price > self.open_price and self.low_price == self.close_price:
                    return 'Bearish_LH_TopWick'
                # Unknown
                else:
                    return '__bearish_LH_UNKNOWN'
            #Unknown
            else:
                return '__bearish_UNKNOWN'
        # Doji
        elif self.close_price == self.open_price:
            # HL
            if self.high_time < self.low_time:
                # Full Wicks
                if self.high_price > self.open_price and self.low_price < self.close_price:
                    return 'Doji_HL_FullWick'
                # Bottom Wick
                elif self.high_price == self.open_price and self.low_price < self.close_price:
                    return 'Doji_HL_BottomWick'
                # Unknown
                else:
                    return 'doji_HL_UNKNOWN'
            # LH
            elif self.low_time < self.high_time:
                # Full Wicks
                if self.high_price > self.open_price and self.low_price < self.close_price:
                    return 'Doji_LH_FullWick'
                # Top Wick
                elif self.high_price > self.open_price and self.low_price == self.close_price:
                    return 'Doji_LH_TopWick'
                # Unknown
                else:
                    return 'doji_LH_UNKNOWN'
            # Single_Event
            elif self.high_time == self.low_time:
                return 'Single_Event'
        else:
            return "UNKNOWN"

    def _refined_candle_classification(self, candle_type: str, group) -> str:
        if candle_type in ('Bullish_LH_NoWick', 'Bearish_HL_NoWick', 'Doji_HL_BottomWick', 'Doji_LH_TopWick', 'Single_Event'):
            return candle_type

        # Initialization
        # For LH Formations
        self.High_Between_Open_Low_price = np.nan
        self.High_Between_Open_Low_time = pd.NaT
        self.Low_Between_High_Close_price = np.nan
        self.Low_Between_High_Close_time = pd.NaT
        # For HL Formations
        self.Low_Between_Open_High_price = np.nan
        self.Low_Between_Open_High_time = pd.NaT
        self.High_Between_Low_Close_price = np.nan
        self.High_Between_Low_Close_time = pd.NaT

        #LH Formations (Bullish & Bearish & Doji)
        if candle_type in ('Bullish_LH_FullWick', 'Bullish_LH_TopWick', 'Bullish_LH_BottomWick', 'Bearish_LH_FullWick', 'Bearish_LH_TopWick', 'Doji_LH_FullWick'):
            self.High_Between_Open_Low_price = group.loc[self.open_time:self.low_time]['price'].max()
            self.High_Between_Open_Low_time = group.loc[self.open_time:self.low_time]['price'].idxmax()
            self.Low_Between_High_Close_price = group.loc[self.high_time:self.close_time]['price'].min()
            self.Low_Between_High_Close_time = group.loc[self.high_time:self.close_time]['price'].idxmin()

            # LH Bullish
            if candle_type == 'Bullish_LH_FullWick':
                if self.High_Between_Open_Low_price > self.open_price and self.Low_Between_High_Close_price < self.close_price:
                    return 'Bullish_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose'
                elif self.High_Between_Open_Low_price > self.open_price:
                    return 'Bullish_LH_FullWick_OpenUpDownToLow'
                elif self.Low_Between_High_Close_price < self.close_price:
                    return 'Bullish_LH_FullWick_PostHighDipBelowClose'
                else:
                    return 'Bullish_LH_FullWick'

            elif candle_type == 'Bullish_LH_TopWick':
                if self.Low_Between_High_Close_price < self.close_price:
                    return 'Bullish_LH_TopWick_PostHighDipBelowClose'
                else:
                    return 'Bullish_LH_TopWick'

            elif candle_type == 'Bullish_LH_BottomWick':
                if self.High_Between_Open_Low_price > self.open_price:
                    return 'Bullish_LH_BottomWick_OpenUpDownToLow'
                else:
                    return 'Bullish_LH_BottomWick'

            # LH Bearish
            elif candle_type == 'Bearish_LH_FullWick':
                if self.High_Between_Open_Low_price > self.open_price and self.Low_Between_High_Close_price < self.close_price:
                    return 'Bearish_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose'
                elif self.High_Between_Open_Low_price > self.open_price:
                    return 'Bearish_LH_FullWick_OpenUpDownToLow'
                elif self.Low_Between_High_Close_price < self.close_price:
                    return 'Bearish_LH_FullWick_PostHighDipBelowClose'
                else:
                    return 'Bearish_LH_FullWick'

            elif candle_type == 'Bearish_LH_TopWick':
                if self.High_Between_Open_Low_price > self.open_price:
                    return 'Bearish_LH_TopWick_OpenUpDownToLow'
                else:
                    return 'Bearish_LH_TopWick'

            # LH Doji
            elif candle_type == 'Doji_LH_FullWick':
                if self.High_Between_Open_Low_price > self.open_price and self.Low_Between_High_Close_price < self.close_price:
                    return 'Doji_LH_FullWick_OpenUpDownToLow_PostHighDipBelowClose'
                elif self.High_Between_Open_Low_price > self.open_price:
                    return 'Doji_LH_FullWick_OpenUpDownToLow'
                elif self.Low_Between_High_Close_price < self.close_price:
                    return 'Doji_LH_FullWick_PostHighDipBelowClose'
                else:
                    return 'Doji_LH_FullWick'

        #HL Formations (Bearish & Bullish & Doji)
        elif candle_type in ('Bearish_HL_FullWick', 'Bearish_HL_BottomWick', 'Bearish_HL_TopWick', 'Bullish_HL_FullWick', 'Bullish_HL_BottomWick', 'Doji_HL_FullWick'):
            self.Low_Between_Open_High_price = group.loc[self.open_time:self.high_time]['price'].min()
            self.Low_Between_Open_High_time = group.loc[self.open_time:self.high_time]['price'].idxmin()
            self.High_Between_Low_Close_price = group.loc[self.low_time:self.close_time]['price'].max()
            self.High_Between_Low_Close_time = group.loc[self.low_time:self.close_time]['price'].idxmax()

            # HL Bearish
            if candle_type == 'Bearish_HL_FullWick':
                if self.Low_Between_Open_High_price < self.open_price and self.High_Between_Low_Close_price > self.close_price:
                    return 'Bearish_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose'
                elif self.Low_Between_Open_High_price < self.open_price:
                    return 'Bearish_HL_FullWick_OpenDownThenUpToHigh'
                elif self.High_Between_Low_Close_price > self.close_price:
                    return 'Bearish_HL_FullWick_PostLowRallyAboveClose'
                else:
                    return 'Bearish_HL_FullWick'

            elif candle_type == 'Bearish_HL_BottomWick':
                if self.High_Between_Low_Close_price > self.close_price:
                    return 'Bearish_HL_BottomWick_PostLowRallyAboveClose'
                else:
                    return 'Bearish_HL_BottomWick'

            elif candle_type == 'Bearish_HL_TopWick':
                if self.Low_Between_Open_High_price < self.open_price:
                    return 'Bearish_HL_TopWick_OpenDownThenUpToHigh'
                else:
                    return 'Bearish_HL_TopWick'

            # HL Bullish
            elif candle_type == 'Bullish_HL_FullWick':
                if self.Low_Between_Open_High_price < self.open_price and self.High_Between_Low_Close_price > self.close_price:
                    return 'Bullish_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose'
                elif self.Low_Between_Open_High_price < self.open_price:
                    return 'Bullish_HL_FullWick_OpenDownThenUpToHigh'
                elif self.High_Between_Low_Close_price > self.close_price:
                    return 'Bullish_HL_FullWick_PostLowRallyAboveClose'
                else:
                    return 'Bullish_HL_FullWick'

            elif candle_type == 'Bullish_HL_BottomWick':
                if self.Low_Between_Open_High_price < self.open_price:
                    return 'Bullish_HL_BottomWick_OpenDownThenUpToHigh'
                else:
                    return 'Bullish_HL_BottomWick'

            # HL Doji
            elif candle_type == 'Doji_HL_FullWick':
                if self.Low_Between_Open_High_price < self.open_price and self.High_Between_Low_Close_price > self.close_price:
                    return 'Doji_HL_FullWick_OpenDownThenUpToHigh_PostLowRallyAboveClose'
                elif self.Low_Between_Open_High_price < self.open_price:
                    return 'Doji_HL_FullWick_OpenDownThenUpToHigh'
                elif self.High_Between_Low_Close_price > self.close_price:
                    return 'Doji_HL_FullWick_PostLowRallyAboveClose'
                else:
                    return 'Doji_HL_FullWick'


    def _exclude_weekends(self):
        nat_mask = self.resampled_df['open_price'].isna()
        nat_group = (nat_mask != nat_mask.shift()).cumsum()
        self.resampled_df['nat_group'] = nat_group[nat_mask]
        consecutive_nat_df = self.resampled_df[nat_mask].copy()
        nat_group_sizes = consecutive_nat_df.groupby('nat_group').size()
        long_nat_groups = nat_group_sizes[nat_group_sizes > (1440 / self.resample_frequency)].index
        long_nat_periods_mask = self.resampled_df['nat_group'].isin(long_nat_groups)
        rows_to_keep_mask = ~long_nat_periods_mask
        df_filtered = self.resampled_df[rows_to_keep_mask].copy()

        removed_candles = len(self.resampled_df) - len(df_filtered)
        percentage_removed = (removed_candles / len(self.resampled_df)) * 100

        print(f"Number of candles removed due to long NaT periods: {removed_candles}")
        print(f"Percentage of candles removed: {percentage_removed:.2f}%")
        self.resampled_df = df_filtered
        self.weekends_excluded_flag = True

    def _handle_continuation(self) -> None:
        # If connection is (++) and candle2 open is lower than the candle1 close, add 2 pivots.
        if self.connection == ('+','+') and self.candle2['open_price'] < self.candle1['close_price']:
            # First add down arrow pivot(indicating high) at the close of the candle1.
            self.swings.append({
                'time'  : self.candle1['close_time'],
                'price' : self.candle1['close_price'],
                'type'  : 'high'
            })
            # Then add up arrow pivot(indicating low) at the open of the candle2.
            self.swings.append({
                'time'  : self.candle2['open_time'],
                'price' : self.candle2['open_price'],
                'type'  : 'low'
            })
        # If connection is (--) and candle2 open is higher than the candle1 close, add 2 pivots.
        elif self.connection == ('-','-') and self.candle2['open_price'] > self.candle1['close_price']:
            # First add up pivot(indicating low) at the close of the candle1.
            self.swings.append({
                'time' : self.candle1['close_time'],
                'price': self.candle1['close_price'],
                'type' : 'low'
            })
            # Then add down pivot(indicating high) at the open of the candle2.
            self.swings.append({
                'time' : self.candle2['open_time'],
                'price': self.candle2['open_price'],
                'type' : 'high'
            })

        if self.candle2['pivots']: # Candles with wicks
            for pivot in self.candle2['pivots']:
                # LH pivots
                if pivot == 'high_betweenOpenLow':
                    self.swings.append({
                        'time' : self.candle2['high_between_open_low_time'],
                        'price': self.candle2['high_between_open_low_price'],
                        'type' : 'high'
                    })
                elif pivot == 'low_betweenHighClose':
                    self.swings.append({
                        'time' : self.candle2['low_between_high_close_time'],
                        'price': self.candle2['low_between_high_close_price'],
                        'type' : 'low'
                    })
                # HL pivots
                elif pivot == 'low_betweenOpenHigh':
                    self.swings.append({
                        'time' : self.candle2['low_between_open_high_time'],
                        'price': self.candle2['low_between_open_high_price'],
                        'type' : 'low'
                    })
                elif pivot == 'high_betweenLowClose':
                    self.swings.append({
                        'time' : self.candle2['high_between_low_close_time'],
                        'price': self.candle2['high_between_low_close_price'],
                        'type' : 'high'
                    })
                # Standard pivots
                else:
                    self.swings.append({
                        'time'  : self.candle2[ pivot +'_time'],    # high_time / low_time
                        'price' : self.candle2[ pivot + '_price'],  # high_price / low_price
                        'type'  : pivot                             # high / low
                    })

    def _handle_reversal(self) -> None:
        # Appending pivot depending on the higher or lower close/open price
        # If connection is (+-) and candle1 close_price is higher than equal to the candle2 open_price
        if self.connection == ('+', '-') and self.candle1['close_price'] >= self.candle2['open_price']:
            self.swings.append({
                'time'  : self.candle1['close_time'],
                'price' : self.candle1['close_price'],
                'type'  : 'high'
            })
        # If connection is (+-) and candle1 close_price is lower to the candle2 open_price
        elif self.connection == ('+', '-') and self.candle1['close_price'] < self.candle2['open_price']:
            self.swings.append({
                'time'  : self.candle2['open_time'],
                'price' : self.candle2['open_price'],
                'type'  : 'high'
            })
        # If connection is (-+) and candle1 close_price is lower than equal to the candle2 open_price
        if self.connection == ('-', '+') and self.candle1['close_price'] <= self.candle2['open_price']:
            self.swings.append({
                'time'  : self.candle1['close_time'],
                'price' : self.candle1['close_price'],
                'type'  : 'low'
            })
        # If connection is (+-) and candle1 close_price is higher to the candle2 open_price
        elif self.connection == ('-', '+') and self.candle1['close_price'] > self.candle2['open_price']:
            self.swings.append({
                'time'  : self.candle2['open_time'],
                'price' : self.candle2['open_price'],
                'type'  : 'low'
            })
        if self.candle2['pivots']: # Candles with wicks
            for pivot in self.candle2['pivots']:
                # LH pivots
                if pivot == 'high_betweenOpenLow':
                    self.swings.append({
                        'time' : self.candle2['high_between_open_low_time'],
                        'price': self.candle2['high_between_open_low_price'],
                        'type' : 'high'
                    })
                elif pivot == 'low_betweenHighClose':
                    self.swings.append({
                        'time' : self.candle2['low_between_high_close_time'],
                        'price': self.candle2['low_between_high_close_price'],
                        'type' : 'low'
                    })
                # HL pivots
                elif pivot == 'low_betweenOpenHigh':
                    self.swings.append({
                        'time' : self.candle2['low_between_open_high_time'],
                        'price': self.candle2['low_between_open_high_price'],
                        'type' : 'low'
                    })
                elif pivot == 'high_betweenLowClose':
                    self.swings.append({
                        'time' : self.candle2['high_between_low_close_time'],
                        'price': self.candle2['high_between_low_close_price'],
                        'type' : 'high'
                    })
                # Standard pivots
                else:
                    self.swings.append({
                        'time'  : self.candle2[ pivot +'_time'],    # high_time / low_time
                        'price' : self.candle2[ pivot + '_price'],  # high_price / low_price
                        'type'  : pivot                             # high / low
                    })

    def _handle_Single_Event(self) -> None:
        # If connection is (+.) and Single event is higher than equal to the candle1 close,
        if self.connection == ('+','.') and self.candle1['close_price'] <= self.candle2['open_price']:
            # The connection for the single event candle becomes (++), update the candle2.
            self.candle2['connectors'] = ('+','+')
        # If connection is (+.) and Single event is lower than the candle1 close,
        elif self.connection == ('+','.') and self.candle1['close_price'] > self.candle2['open_price']:
            # The connection for the single event candle becomes (--), update the candle2.
            self.candle2['connectors'] = ('-','-')
            # Append marker for the previous close.
            self.swings.append({
                'time'  : self.candle1['close_time'],
                'price' : self.candle1['close_price'],
                'type'  : 'high'
            })
        # If connection is (-.) and Single event is lower than equal to the candle1 close,
        if self.connection == ('-','.') and self.candle1['close_price'] >= self.candle2['open_price']:
            # The connection for the single event candle becomes (--), update the candle2.
            self.candle2['connectors'] = ('-','-')
        # If connection is (+.) and Single event is higher than the candle1 close,
        elif self.connection == ('-','.') and self.candle1['close_price'] < self.candle2['open_price']:
            # The connection for the single event candle becomes (++), update the candle2.
            self.candle2['connectors'] = ('+', '+')
            # Append marker for the previous close.
            self.swings.append({
                'time'  : self.candle1['close_time'],
                'price' : self.candle1['close_price'],
                'type'  : 'low'
            })

