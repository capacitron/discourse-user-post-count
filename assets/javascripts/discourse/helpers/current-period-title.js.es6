const CURRENT_TITLE_SUBS = {
  first_quarterly: 'first_quarter',
  second_quarterly: 'second_quarter',
  third_quarterly: 'third_quarter',
  fourth_quarterly: 'fourth_quarter',
};

export default Ember.Handlebars.makeBoundHelper(function (current_period, options) {
  const title = I18n.t('filters.top.' + (CURRENT_TITLE_SUBS[current_period] || 'first_quarter'));
  if (options.hash.showDateRange) {
    var dateString = "";
    switch(current_period) {
      case 'first_quarterly':
        dateString = moment().startOf('year').format(I18n.t('dates.long_no_year_no_time')) + " - " + moment().startOf('year').add(2, 'month').endOf('month').format(I18n.t('dates.long_no_year_no_time'));
        break;
      case 'second_quarterly':
        dateString = moment().startOf('year').add(3, 'month').startOf('month').format(I18n.t('dates.long_no_year_no_time')) + " - " + moment().startOf('year').add(5, 'month').endOf('month').format(I18n.t('dates.long_no_year_no_time'));
        break;
      case 'third_quarterly':
        dateString = moment().startOf('year').add(6, 'month').startOf('month').format(I18n.t('dates.long_no_year_no_time')) + " - " + moment().startOf('year').add(8, 'month').endOf('month').format(I18n.t('dates.long_no_year_no_time'));
        break;
      case 'fourth_quarterly':
        dateString = moment().startOf('year').add(9, 'month').startOf('month').format(I18n.t('dates.long_no_year_no_time')) + " - " + moment().endOf('year').format(I18n.t('dates.long_no_year_no_time'));
        break;
    }
    return new Handlebars.SafeString(title + " <span class='top-date-string'>" + dateString + "</span>");
  } else {
    return new Handlebars.SafeString(title);
  }
});
