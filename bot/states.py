from aiogram.fsm.state import StatesGroup, State

class ReturnCode(StatesGroup):
    waiting_for_file = State()
