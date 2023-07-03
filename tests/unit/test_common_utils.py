from andb.common.utils import get_the_nearest_two_power_number


def test_tow_power():
    assert (get_the_nearest_two_power_number(3)) == 4
    assert (get_the_nearest_two_power_number(5)) == 8
    assert (bin(get_the_nearest_two_power_number(0xffffffff))) == '0b10000000000000000000000000000000'