import { createI18n } from 'vue-i18n'
import zh from './zh'
import en from './en'

const savedLocale = localStorage.getItem('dmqtt-locale') || 'zh'

const i18n = createI18n({
  legacy: false,
  locale: savedLocale,
  fallbackLocale: 'zh',
  messages: { zh, en },
})

export default i18n
